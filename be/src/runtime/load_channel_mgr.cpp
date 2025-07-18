// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_channel_mgr.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/load_channel_mgr.h"

#include <brpc/controller.h>
#include <butil/endpoint.h>

#include <memory>

#include "common/closure_guard.h"
#include "fs/key_cache.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "storage/lake/tablet_manager.h"
#include "storage/utils.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"

namespace starrocks {

class ChannelOpenTask final : public Runnable {
public:
    ChannelOpenTask(LoadChannelMgr* load_channel_mgr, LoadChannelOpenContext open_context)
            : _load_channel_mgr(load_channel_mgr), _open_context(std::move(open_context)) {}

    ~ChannelOpenTask() override {
        if (!_is_done) {
            cancel_task(Status::ServiceUnavailable("Thread pool was shut down"));
        }
    }

    void run() override {
        if (_try_mark_done()) {
            _load_channel_mgr->_open(_open_context);
        }
    }

    void cancel_task(const Status& status) {
        if (_try_mark_done()) {
            ClosureGuard closure_guard(_open_context.done);
            status.to_protobuf(_open_context.response->mutable_status());
        }
    }

private:
    bool _try_mark_done() {
        bool old = false;
        return _is_done.compare_exchange_strong(old, true);
    }

    LoadChannelMgr* _load_channel_mgr;
    LoadChannelOpenContext _open_context;
    std::atomic<bool> _is_done{false};
};

// Calculate the memory limit for a single load job.
static int64_t calc_job_max_load_memory(int64_t mem_limit_in_req, int64_t total_mem_limit) {
    // mem_limit_in_req == -1 means no limit for single load.
    // total_mem_limit according to load_process_max_memory_limit_percent calculation
    if (mem_limit_in_req <= 0) {
        return total_mem_limit;
    }
    return std::min<int64_t>(mem_limit_in_req, total_mem_limit);
}

static int64_t calc_job_timeout_s(int64_t timeout_in_req_s) {
    int64_t load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    if (timeout_in_req_s > 0) {
        load_channel_timeout_s = std::max<int64_t>(load_channel_timeout_s, timeout_in_req_s);
    }
    return load_channel_timeout_s;
}

LoadChannelMgr::LoadChannelMgr() : _mem_tracker(nullptr), _load_channels_clean_thread(INVALID_BTHREAD) {
    REGISTER_GAUGE_STARROCKS_METRIC(load_channel_count, [this]() {
        std::lock_guard l(_lock);
        return _load_channels.size();
    });
}

LoadChannelMgr::~LoadChannelMgr() {
    if (_load_channels_clean_thread != INVALID_BTHREAD) {
        [[maybe_unused]] void* ret;
        bthread_stop(_load_channels_clean_thread);
        bthread_join(_load_channels_clean_thread, &ret);
    }
}

void LoadChannelMgr::close() {
    _async_rpc_pool->shutdown();
    std::lock_guard l(_lock);
    for (auto iter = _load_channels.begin(); iter != _load_channels.end();) {
        iter->second->cancel();
        iter->second->abort();
        iter = _load_channels.erase(iter);
    }
}

Status LoadChannelMgr::init(MemTracker* mem_tracker) {
    _mem_tracker = mem_tracker;
    RETURN_IF_ERROR(_start_bg_worker());
    int num_threads = config::load_channel_rpc_thread_pool_num;
    if (num_threads <= 0) {
        num_threads = CpuInfo::num_cores();
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("load_channel")
                            .set_min_threads(std::min(5, num_threads))
                            .set_max_threads(num_threads)
                            .set_max_queue_size(config::load_channel_rpc_thread_pool_queue_size)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(60000))
                            .build(&_async_rpc_pool));
    REGISTER_THREAD_POOL_METRICS(load_channel, _async_rpc_pool.get());
    return Status::OK();
}

void LoadChannelMgr::open(brpc::Controller* cntl, const PTabletWriterOpenRequest& request,
                          PTabletWriterOpenResult* response, google::protobuf::Closure* done) {
    LoadChannelOpenContext open_context;
    open_context.cntl = cntl;
    open_context.request = &request;
    open_context.response = response;
    open_context.done = done;
    open_context.receive_rpc_time_ns = MonotonicNanos();
    if (!config::enable_load_channel_rpc_async) {
        _open(open_context);
        return;
    }
    auto task = std::make_shared<ChannelOpenTask>(this, std::move(open_context));
    Status status = _async_rpc_pool->submit(task);
    if (!status.ok()) {
        task->cancel_task(status);
    }
}

void LoadChannelMgr::_open(LoadChannelOpenContext open_context) {
    ClosureGuard done_guard(open_context.done);
    const PTabletWriterOpenRequest& request = *open_context.request;
    PTabletWriterOpenResult* response = open_context.response;
    if (!request.encryption_meta().empty()) {
        Status st = KeyCache::instance().refresh_keys(request.encryption_meta());
        if (!st.ok()) {
            response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
            response->mutable_status()->add_error_msgs(fmt::format(
                    "refresh keys using encryption_meta in PTabletWriterOpenRequest failed {}", st.detailed_message()));
            return;
        }
    }
    UniqueId load_id(request.id());
    int64_t txn_id = request.txn_id();
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard l(_lock);
        auto it = _load_channels.find(load_id);
        if (it != _load_channels.end()) {
            channel = it->second;
        } else if (!is_tracker_hit_hard_limit(_mem_tracker, config::load_process_max_memory_hard_limit_ratio) ||
                   config::enable_new_load_on_memory_limit_exceeded) {
            // When loading memory usage is larger than hard limit, we will reject new loading task.
            int64_t mem_limit_in_req = request.has_load_mem_limit() ? request.load_mem_limit() : -1;
            int64_t job_max_memory = calc_job_max_load_memory(mem_limit_in_req, _mem_tracker->limit());

            int64_t timeout_in_req_s = request.has_load_channel_timeout_s() ? request.load_channel_timeout_s() : -1;
            int64_t job_timeout_s = calc_job_timeout_s(timeout_in_req_s);
            auto job_mem_tracker = std::make_unique<MemTracker>(job_max_memory, load_id.to_string(), _mem_tracker);

            channel.reset(new LoadChannel(this, ExecEnv::GetInstance()->lake_tablet_manager(), load_id, txn_id,
                                          request.txn_trace_parent(), job_timeout_s, std::move(job_mem_tracker)));
            if (request.has_load_channel_profile_config()) {
                channel->set_profile_config(request.load_channel_profile_config());
            }
            _load_channels.insert({load_id, channel});
        } else {
            response->mutable_status()->set_status_code(TStatusCode::MEM_LIMIT_EXCEEDED);
            response->mutable_status()->add_error_msgs(
                    "memory limit exceeded, please reduce load frequency or increase config "
                    "`load_process_max_memory_hard_limit_ratio` or add more BE nodes");
            return;
        }
    }
    done_guard.release();
    channel->open(open_context);
}

void LoadChannelMgr::add_chunk(const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    VLOG(2) << "Current memory usage=" << _mem_tracker->consumption() << " limit=" << _mem_tracker->limit();
    UniqueId load_id(request.id());
    auto channel = _find_load_channel(load_id);
    if (channel != nullptr) {
        channel->add_chunk(request, response);
    } else {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("no associated load channel " + print_id(request.id()));
    }
}

void LoadChannelMgr::add_chunks(const PTabletWriterAddChunksRequest& request, PTabletWriterAddBatchResult* response) {
    VLOG(2) << "Current memory usage=" << _mem_tracker->consumption() << " limit=" << _mem_tracker->limit();
    UniqueId load_id(request.id());
    auto channel = _find_load_channel(load_id);
    if (channel != nullptr) {
        channel->add_chunks(request, response);
    } else {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("no associated load channel " + print_id(request.id()));
    }
}

void LoadChannelMgr::add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                                 PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    UniqueId load_id(request->id());
    auto channel = _find_load_channel(load_id);
    if (channel != nullptr) {
        channel->add_segment(cntl, request, response, done);
        closure_guard.release();
    } else {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("no associated load channel " + print_id(request->id()));
    }
}

void LoadChannelMgr::cancel(brpc::Controller* cntl, const PTabletWriterCancelRequest& request,
                            PTabletWriterCancelResult* response, google::protobuf::Closure* done) {
    ClosureGuard done_guard(done);
    UniqueId load_id(request.id());
    if (request.has_tablet_id()) {
        auto channel = _find_load_channel(load_id);
        if (channel != nullptr) {
            channel->abort(TabletsChannelKey(request.id(), request.sink_id(), request.index_id()),
                           {request.tablet_id()}, request.reason());
        }
    } else if (request.tablet_ids_size() > 0) {
        auto channel = _find_load_channel(load_id);
        if (channel != nullptr) {
            std::vector<int64_t> tablet_ids;
            for (auto& tablet_id : request.tablet_ids()) {
                tablet_ids.emplace_back(tablet_id);
            }
            channel->abort(TabletsChannelKey(request.id(), request.sink_id(), request.index_id()), tablet_ids,
                           request.reason());
        }
    } else {
        if (auto channel = remove_load_channel(load_id); channel != nullptr) {
            channel->cancel();
            channel->abort();
        }
    }
}

void LoadChannelMgr::get_load_replica_status(brpc::Controller* cntl, const PLoadReplicaStatusRequest* request,
                                             PLoadReplicaStatusResult* response, google::protobuf::Closure* done) {
    ClosureGuard done_guard(done);
    UniqueId load_id(request->load_id());
    auto channel = _find_load_channel(load_id);
    if (channel == nullptr) {
        for (int64_t tablet_id : request->tablet_ids()) {
            auto replica_status = response->add_replica_statuses();
            replica_status->set_tablet_id(tablet_id);
            replica_status->set_state(LoadReplicaStatePB::NOT_PRESENT);
            replica_status->set_message("can't find load channel");
        }
    } else {
        std::string remote_ip = butil::ip2str(cntl->remote_side().ip).c_str();
        channel->get_load_replica_status(remote_ip, request, response);
    }
}

void LoadChannelMgr::load_diagnose(brpc::Controller* cntl, const PLoadDiagnoseRequest* request,
                                   PLoadDiagnoseResult* response, google::protobuf::Closure* done) {
    ClosureGuard done_guard(done);
    UniqueId load_id(request->id());
    auto channel = _find_load_channel(load_id);
    if (channel == nullptr) {
        if (request->has_profile() && request->profile()) {
            response->mutable_profile_status()->set_status_code(TStatusCode::NOT_FOUND);
            response->mutable_profile_status()->add_error_msgs("can't find the load channel");
        }
        if (request->has_stack_trace() && request->stack_trace()) {
            response->mutable_stack_trace_status()->set_status_code(TStatusCode::NOT_FOUND);
            response->mutable_stack_trace_status()->add_error_msgs("can't find the load channel");
        }
    } else {
        std::string remote_ip = butil::ip2str(cntl->remote_side().ip).c_str();
        LOG(INFO) << "receive load diagnose, load_id: " << load_id << ", txn_id: " << request->txn_id()
                  << ", remote: " << remote_ip;
        channel->diagnose(remote_ip, request, response);
    }
}

void* LoadChannelMgr::load_channel_clean_bg_worker(void* arg) {
#ifndef BE_TEST
    uint64_t interval = 60;
#else
    uint64_t interval = 1;
#endif
    auto mgr = static_cast<LoadChannelMgr*>(arg);
    while (!bthread_stopped(bthread_self())) {
        if (bthread_usleep(interval * 1000 * 1000) == 0) {
            mgr->_start_load_channels_clean();
        }
    }
    return nullptr;
}

Status LoadChannelMgr::_start_bg_worker() {
    int r = bthread_start_background(&_load_channels_clean_thread, nullptr, load_channel_clean_bg_worker, this);
    if (UNLIKELY(r != 0)) {
        PLOG(ERROR) << "Fail to create bthread.";
        return Status::InternalError("Fail to create bthread");
    }
    return Status::OK();
}

void LoadChannelMgr::_start_load_channels_clean() {
    std::vector<std::shared_ptr<LoadChannel>> timeout_channels;

    time_t now = time(nullptr);
    {
        std::lock_guard l(_lock);
        for (auto it = _load_channels.begin(); it != _load_channels.end(); /**/) {
            if (difftime(now, it->second->last_updated_time()) >= it->second->timeout()) {
                timeout_channels.emplace_back(std::move(it->second));
                it = _load_channels.erase(it);
            } else {
                ++it;
            }
        }
    }

    // we must cancel these load channels before destroying them
    // otherwise some object may be invalid before trying to visit it.
    // eg: MemTracker in load channel
    for (auto& channel : timeout_channels) {
        channel->cancel();
    }
    for (auto& channel : timeout_channels) {
        channel->abort();
        LOG(INFO) << "Deleted timeout channel. load id=" << channel->load_id() << " timeout=" << channel->timeout();
    }

    // clean load in writing data size
    if (auto lake_tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager(); lake_tablet_manager != nullptr) {
        lake_tablet_manager->clean_in_writing_data_size();
    }
}

std::shared_ptr<LoadChannel> LoadChannelMgr::_find_load_channel(const UniqueId& load_id) {
    std::lock_guard l(_lock);
    auto it = _load_channels.find(load_id);
    return (it != _load_channels.end()) ? it->second : nullptr;
}

std::shared_ptr<LoadChannel> LoadChannelMgr::_find_load_channel(int64_t txn_id) {
    std::lock_guard l(_lock);
    for (auto&& [load_id, channel] : _load_channels) {
        if (channel->txn_id() == txn_id) return channel;
    }
    return nullptr;
}

std::shared_ptr<LoadChannel> LoadChannelMgr::remove_load_channel(const UniqueId& load_id) {
    std::lock_guard l(_lock);
    if (auto it = _load_channels.find(load_id); it != _load_channels.end()) {
        auto ret = it->second;
        _load_channels.erase(it);
        return ret;
    }
    return nullptr;
}

void LoadChannelMgr::abort_txn(int64_t txn_id) {
    auto channel = _find_load_channel(txn_id);
    if (channel != nullptr) {
        LOG(INFO) << "Aborting load channel because transaction was aborted. load_id=" << channel->load_id()
                  << " txn_id=" << txn_id;
        channel->cancel();
        channel->abort();
    }
}

} // namespace starrocks
