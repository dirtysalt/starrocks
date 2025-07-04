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
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/task_worker_pool.cpp

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

#include "agent/task_worker_pool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <sstream>
#include <string>

#include "agent/agent_server.h"
#include "agent/finish_task.h"
#include "agent/master_info.h"
#include "agent/publish_version.h"
#include "agent/report_task.h"
#include "agent/resource_group_usage_recorder.h"
#include "agent/task_signatures_manager.h"
#include "cache/datacache.h"
#include "cache/datacache_utils.h"
#include "common/status.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/work_group.h"
#include "fs/fs_util.h"
#include "gen_cpp/DataCache_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "storage/data_dir.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/publish_version_manager.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/task/engine_batch_load_task.h"
#include "storage/task/engine_clone_task.h"
#include "storage/update_manager.h"
#include "storage/utils.h"
#include "util/misc.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"

namespace starrocks {

namespace {
static void wait_for_disk_report_notify(const std::function<bool()>& stop_waiting) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(config::report_disk_state_interval_seconds);
    bool notified = false;
    do {
        // take 1 second per step
        notified = StorageEngine::instance()->wait_for_report_notify(1, false);
    } while (!notified && std::chrono::steady_clock::now() < deadline && !stop_waiting());
}

static void wait_for_tablet_report_notify(const std::function<bool()>& stop_waiting) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(config::report_tablet_interval_seconds);
    bool notified = false;
    do {
        // take 1 second per step
        notified = StorageEngine::instance()->wait_for_report_notify(1, true);
    } while (!notified
             // if the regular report is stopped, there will be no deadline
             && (ReportOlapTableTaskWorkerPool::is_regular_report_stopped() ||
                 std::chrono::steady_clock::now() < deadline) &&
             !stop_waiting());
}
} // namespace

const size_t PUBLISH_VERSION_BATCH_SIZE = 10;

std::atomic<int64_t> g_report_version(time(nullptr) * 10000);

using std::swap;

int64_t curr_report_version() {
    return g_report_version.load();
}

int64_t next_report_version() {
    return ++g_report_version;
}

template <class AgentTaskRequest>
TaskWorkerPool<AgentTaskRequest>::TaskWorkerPool(ExecEnv* env, int worker_count)
        : _env(env), _worker_thread_condition_variable(new std::condition_variable()), _worker_count(worker_count) {
    _backend.__set_host(BackendOptions::get_localhost());
    _backend.__set_be_port(config::be_port);
    _backend.__set_http_port(config::be_http_port);
}

template <class AgentTaskRequest>
TaskWorkerPool<AgentTaskRequest>::~TaskWorkerPool() {
    stop();
    for (uint32_t i = 0; i < _worker_count; ++i) {
        if (_worker_threads[i].joinable()) {
            _worker_threads[i].join();
        }
    }
    delete _worker_thread_condition_variable;
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::start() {
    for (uint32_t i = 0; i < _worker_count; i++) {
        _spawn_callback_worker_thread(_callback_function);
    }
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::stop() {
    _stopped = true;
    _worker_thread_condition_variable->notify_all();
}

template <class AgentTaskRequest>
size_t TaskWorkerPool<AgentTaskRequest>::_push_task(AgentTaskRequestPtr task) {
    std::unique_lock l(_worker_thread_lock);
    _tasks.emplace_back(std::move(task));
    _worker_thread_condition_variable->notify_one();
    return _tasks.size();
}

template <class AgentTaskRequest>
typename TaskWorkerPool<AgentTaskRequest>::AgentTaskRequestPtr TaskWorkerPool<AgentTaskRequest>::_pop_task() {
    std::unique_lock l(_worker_thread_lock);
    _worker_thread_condition_variable->wait(l, [&]() { return !_tasks.empty() || _stopped; });
    if (!_stopped) {
        auto ret = std::move(_tasks.front());
        _tasks.pop_front();
        return ret;
    }
    return nullptr;
}

template <class AgentTaskRequest>
typename TaskWorkerPool<AgentTaskRequest>::AgentTaskRequestPtr TaskWorkerPool<AgentTaskRequest>::_pop_task(
        TPriority::type pri) {
    std::unique_lock l(_worker_thread_lock);
    _worker_thread_condition_variable->wait(l, [&]() { return !_tasks.empty() || _stopped; });
    if (_stopped) {
        return nullptr;
    }
    for (int64_t i = static_cast<int64_t>(_tasks.size()) - 1; i >= 0; --i) {
        auto& task = _tasks[i];
        if (task->isset.priority && task->priority == pri) {
            auto ret = std::move(task);
            _tasks.erase(_tasks.begin() + i);
            return ret;
        }
    }
    return nullptr;
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::submit_task(const TAgentTaskRequest& task) {
    const TTaskType::type task_type = task.task_type;
    int64_t signature = task.signature;

    std::string type_str;
    EnumToString(TTaskType, task_type, type_str);

    std::pair<bool, size_t> register_pair = register_task_info(task_type, signature);
    if (register_pair.first) {
        // Set the receiving time of task so that we can determine whether it is timed out later
        auto new_task = _convert_task(task, time(nullptr));
        size_t task_count = _push_task(std::move(new_task));
        VLOG(1) << "Submit task success. type=" << type_str << ", signature=" << signature
                << ", task_count_in_queue=" << task_count;
    } else {
        LOG(INFO) << "Submit task failed, already exists type=" << type_str << ", signature=" << signature;
    }
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::submit_tasks(const std::vector<const TAgentTaskRequest*>& tasks) {
    DCHECK(!tasks.empty());
    std::string type_str;
    size_t task_count = tasks.size();
    const TTaskType::type task_type = tasks[0]->task_type;
    EnumToString(TTaskType, task_type, type_str);
    const auto recv_time = time(nullptr);
    auto failed_task = batch_register_task_info(tasks);

    size_t non_zeros = SIMD::count_nonzero(failed_task);

    std::stringstream fail_ss;
    std::stringstream succ_ss;
    size_t fail_counter = non_zeros;
    size_t succ_counter = task_count - non_zeros;
    for (int i = 0; i < failed_task.size(); ++i) {
        if (failed_task[i] == 1) {
            fail_ss << tasks[i]->signature;
            fail_counter--;
            if (fail_counter > 0) {
                fail_ss << ",";
            }
        } else {
            succ_ss << tasks[i]->signature;
            succ_counter--;
            if (succ_counter > 0) {
                succ_ss << ",";
            }
        }
    }
    if (fail_ss.str().length() > 0) {
        LOG(INFO) << "fail to register task. type=" << type_str << ", signatures=[" << fail_ss.str() << "]";
    }

    size_t queue_size = 0;
    {
        std::unique_lock l(_worker_thread_lock);
        if (UNLIKELY(task_type == TTaskType::REALTIME_PUSH &&
                     tasks[0]->push_req.push_type == TPushType::CANCEL_DELETE)) {
            for (size_t i = 0; i < task_count; i++) {
                if (failed_task[i] == 0) {
                    auto new_task = _convert_task(*tasks[i], recv_time);
                    _tasks.emplace_front(std::move(new_task));
                }
            }
        } else {
            for (size_t i = 0; i < task_count; i++) {
                if (failed_task[i] == 0) {
                    auto new_task = _convert_task(*tasks[i], recv_time);
                    _tasks.emplace_back(std::move(new_task));
                }
            }
        }
        queue_size = _tasks.size();
        _worker_thread_condition_variable->notify_all();
    }

    if (succ_ss.str().length() > 0) {
        LOG(INFO) << "success to submit task. type=" << type_str << ", signature=[" << succ_ss.str()
                  << "], task_count_in_queue=" << queue_size;
    }
}

template <class AgentTaskRequest>
size_t TaskWorkerPool<AgentTaskRequest>::num_queued_tasks() const {
    std::lock_guard l(_worker_thread_lock);
    return _tasks.size();
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::_spawn_callback_worker_thread(CALLBACK_FUNCTION callback_func) {
    std::thread worker_thread(callback_func, this);
    _worker_threads.emplace_back(std::move(worker_thread));
    Thread::set_thread_name(_worker_threads.back(), "task_worker");
}

void* PushTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    static uint32_t s_worker_count = 0;

    auto* worker_pool_this = (PushTaskWorkerPool*)arg_this;

    TPriority::type priority = TPriority::NORMAL;

    int32_t push_worker_count_high_priority = config::push_worker_count_high_priority;
    {
        std::lock_guard worker_thread_lock(worker_pool_this->_worker_thread_lock);
        if (s_worker_count < push_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

    while (true) {
        AgentStatus status = STARROCKS_SUCCESS;
        AgentTaskRequestPtr agent_task_req;
        do {
            agent_task_req = worker_pool_this->_pop_task(priority);
            if (agent_task_req == nullptr) {
                // there is no high priority task. notify other thread to handle normal task
                worker_pool_this->_worker_thread_condition_variable->notify_one();
                break;
            }
        } while (false);

        if (worker_pool_this->_stopped) {
            break;
        }
        if (agent_task_req == nullptr) {
            // there is no high priority task in queue
            sleep(1);
            continue;
        }
        auto& push_req = agent_task_req->task_req;

        LOG(INFO) << "get push task. signature: " << agent_task_req->signature << " priority: " << priority
                  << " push_type: " << push_req.push_type;
        std::vector<TTabletInfo> tablet_infos;

        EngineBatchLoadTask engine_task(push_req, &tablet_infos, agent_task_req->signature, &status,
                                        GlobalEnv::GetInstance()->load_mem_tracker());
        // EngineBatchLoadTask execute always return OK
        (void)(StorageEngine::instance()->execute_task(&engine_task));

        if (status == STARROCKS_PUSH_HAD_LOADED) {
            // remove the task and not return to fe
            remove_task_info(agent_task_req->task_type, agent_task_req->signature);
            continue;
        }
        // Return result to fe
        std::vector<string> error_msgs;
        TStatus task_status;

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_task_type(agent_task_req->task_type);
        finish_task_request.__set_signature(agent_task_req->signature);

        if (status == STARROCKS_SUCCESS) {
            VLOG(3) << "push ok. signature: " << agent_task_req->signature << ", push_type: " << push_req.push_type;
            error_msgs.emplace_back("push success");

            g_report_version.fetch_add(1, std::memory_order_relaxed);

            task_status.__set_status_code(TStatusCode::OK);
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        } else if (status == STARROCKS_TASK_REQUEST_ERROR) {
            LOG(WARNING) << "push request push_type invalid. type: " << push_req.push_type
                         << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("push request push_type invalid.");
            task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
        } else {
            LOG(WARNING) << "push failed, error_code: " << status << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("push failed");
            task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        }
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_report_version(g_report_version.load(std::memory_order_relaxed));

        finish_task(finish_task_request);
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }

    return nullptr;
}

void* DeleteTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    static uint32_t s_worker_count = 0;

    auto* worker_pool_this = (DeleteTaskWorkerPool*)arg_this;

    TPriority::type priority = TPriority::NORMAL;

    int32_t delete_worker_count_high_priority = config::delete_worker_count_high_priority;
    {
        std::lock_guard worker_thread_lock(worker_pool_this->_worker_thread_lock);
        if (s_worker_count < delete_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

    while (true) {
        AgentStatus status = STARROCKS_SUCCESS;
        AgentTaskRequestPtr agent_task_req;
        do {
            agent_task_req = worker_pool_this->_pop_task(priority);
            if (agent_task_req == nullptr) {
                // there is no high priority task. notify other thread to handle normal task
                worker_pool_this->_worker_thread_condition_variable->notify_one();
                break;
            }
            const auto& push_req = agent_task_req->task_req;

            int num_of_remove_task = 0;
            if (push_req.push_type == TPushType::CANCEL_DELETE) {
                LOG(INFO) << "get delete push task. remove delete task txn_id: " << push_req.transaction_id
                          << " priority: " << priority << " push_type: " << push_req.push_type;

                std::lock_guard l(worker_pool_this->_worker_thread_lock);
                auto& tasks = worker_pool_this->_tasks;
                for (auto it = tasks.begin(); it != tasks.end();) {
                    AgentTaskRequestPtr& task_req = *it;
                    if (task_req->task_type == TTaskType::REALTIME_PUSH) {
                        const TPushReq& push_task_in_queue = task_req->task_req;
                        if (push_task_in_queue.push_type == TPushType::DELETE &&
                            push_task_in_queue.transaction_id == push_req.transaction_id) {
                            it = worker_pool_this->_tasks.erase(it);
                            ++num_of_remove_task;
                        } else {
                            ++it;
                        }
                    }
                }
                LOG(INFO) << "finish remove delete task txn_id: " << push_req.transaction_id
                          << " num_of_remove_task: " << num_of_remove_task << " priority: " << priority
                          << " push_type: " << push_req.push_type;
            }
        } while (false);

        if (worker_pool_this->_stopped) {
            break;
        }
        if (agent_task_req == nullptr) {
            // there is no high priority task in queue
            sleep(1);
            continue;
        }
        auto& push_req = agent_task_req->task_req;

        LOG(INFO) << "get delete push task. signature: " << agent_task_req->signature << " priority: " << priority
                  << " push_type: " << push_req.push_type;
        std::vector<TTabletInfo> tablet_infos;
        EngineBatchLoadTask engine_task(push_req, &tablet_infos, agent_task_req->signature, &status,
                                        GlobalEnv::GetInstance()->load_mem_tracker());
        // EngineBatchLoadTask execute always return OK
        (void)(StorageEngine::instance()->execute_task(&engine_task));

        if (status == STARROCKS_PUSH_HAD_LOADED) {
            // remove the task and not return to fe
            remove_task_info(agent_task_req->task_type, agent_task_req->signature);
            continue;
        }
        // Return result to fe
        std::vector<string> error_msgs;
        TStatus task_status;

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_task_type(agent_task_req->task_type);
        finish_task_request.__set_signature(agent_task_req->signature);
        if (push_req.push_type == TPushType::DELETE) {
            finish_task_request.__set_request_version(push_req.version);
        }

        if (status == STARROCKS_SUCCESS) {
            VLOG(3) << "delete push ok. signature: " << agent_task_req->signature
                    << ", push_type: " << push_req.push_type;
            error_msgs.emplace_back("push success");

            g_report_version.fetch_add(1, std::memory_order_relaxed);

            task_status.__set_status_code(TStatusCode::OK);
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        } else if (status == STARROCKS_TASK_REQUEST_ERROR) {
            LOG(WARNING) << "delete push request push_type invalid. type: " << push_req.push_type
                         << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("push request push_type invalid.");
            task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
        } else {
            LOG(WARNING) << "delete push failed, error_code: " << status
                         << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("delete push failed");
            task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        }
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_report_version(g_report_version.load(std::memory_order_relaxed));

        finish_task(finish_task_request);
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }

    return nullptr;
}

void* PublishVersionTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (PublishVersionTaskWorkerPool*)arg_this;
    auto* agent_server = worker_pool_this->_env->agent_server();
    auto token =
            agent_server->get_thread_pool(TTaskType::PUBLISH_VERSION)->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    struct VersionCmp {
        bool operator()(const AgentTaskRequestPtr& lhs, const AgentTaskRequestPtr& rhs) const {
            if (lhs->task_req.__isset.commit_timestamp && rhs->task_req.__isset.commit_timestamp) {
                if (lhs->task_req.commit_timestamp > rhs->task_req.commit_timestamp) {
                    return true;
                }
            }
            return false;
        }
    };
    std::priority_queue<AgentTaskRequestPtr, std::vector<AgentTaskRequestPtr>, VersionCmp> priority_tasks;
    std::unordered_set<DataDir*> affected_dirs;
    std::vector<TFinishTaskRequest> finish_task_requests;
    int64_t batch_publish_latency = 0;
    std::vector<int64_t> finished_txn;
    while (true) {
        uint32_t wait_time = config::wait_apply_time;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            worker_pool_this->_sleeping_count++;
            worker_pool_this->_worker_thread_condition_variable->wait(l, [&]() {
                return !priority_tasks.empty() || !worker_pool_this->_tasks.empty() || worker_pool_this->_stopped;
            });
            worker_pool_this->_sleeping_count--;
            if (worker_pool_this->_stopped) {
                break;
            }
            // All thread are running, set wait_timeout = 0 to avoid publish block
            wait_time = wait_time * worker_pool_this->_sleeping_count / worker_pool_this->_worker_count;

            while (!worker_pool_this->_tasks.empty()) {
                // collect some publish version tasks as a group.
                priority_tasks.emplace(std::move(worker_pool_this->_tasks.front()));
                worker_pool_this->_tasks.pop_front();
            }
        }

        const auto& publish_version_task = *priority_tasks.top();
        VLOG(1) << "get publish version task txn_id: " << publish_version_task.task_req.transaction_id
                << " priority queue size: " << priority_tasks.size();
        bool enable_sync_publish = publish_version_task.task_req.enable_sync_publish;
        if (enable_sync_publish) {
            wait_time = 0;
        }
        StarRocksMetrics::instance()->publish_task_request_total.increment(1);
        auto& finish_task_request = finish_task_requests.emplace_back();
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_report_version(g_report_version.load(std::memory_order_relaxed));
        int64_t start_ts = MonotonicMillis();
        run_publish_version_task(token.get(), publish_version_task.task_req, finish_task_request, affected_dirs,
                                 wait_time);
        finish_task_request.__set_task_type(publish_version_task.task_type);
        finish_task_request.__set_signature(publish_version_task.signature);

        batch_publish_latency += MonotonicMillis() - start_ts;
        finished_txn.emplace_back(publish_version_task.task_req.transaction_id);
        priority_tasks.pop();

        if (!enable_sync_publish) {
            if (priority_tasks.empty() || finish_task_requests.size() > PUBLISH_VERSION_BATCH_SIZE ||
                batch_publish_latency > config::max_batch_publish_latency_ms) {
                int64_t t0 = MonotonicMillis();
                StorageEngine::instance()->txn_manager()->flush_dirs(affected_dirs);
                int64_t t1 = MonotonicMillis();
                // notify FE when all tasks of group have been finished.
                for (auto& finish_task_request : finish_task_requests) {
                    finish_task(finish_task_request);
                    remove_task_info(finish_task_request.task_type, finish_task_request.signature);
                }
                int64_t t2 = MonotonicMillis();
                VLOG(1) << "batch flush " << finish_task_requests.size()
                        << " txn publish task(s). #dir:" << affected_dirs.size() << " flush:" << t1 - t0
                        << "ms finish_task_rpc:" << t2 - t1 << "ms";
                finish_task_requests.clear();
                affected_dirs.clear();
                batch_publish_latency = 0;
            }
        } else {
            if (priority_tasks.empty() || finish_task_requests.size() > PUBLISH_VERSION_BATCH_SIZE ||
                batch_publish_latency > config::max_batch_publish_latency_ms) {
                int64_t finish_task_size = finish_task_requests.size();
                int64_t t0 = MonotonicMillis();
                StorageEngine::instance()->txn_manager()->flush_dirs(affected_dirs);
                int64_t t1 = MonotonicMillis();
                StorageEngine::instance()->publish_version_manager()->wait_publish_task_apply_finish(
                        std::move(finish_task_requests));
                StorageEngine::instance()->wake_finish_publish_vesion_thread();
                affected_dirs.clear();
                batch_publish_latency = 0;
                VLOG(1) << "batch submit " << finish_task_size << " finish publish version task "
                        << "txn publish task(s). #dir:" << affected_dirs.size() << " flush:" << t1 - t0 << "ms";
            }
        }
        if (priority_tasks.empty() || finished_txn.size() > PUBLISH_VERSION_BATCH_SIZE) {
            std::stringstream ss;
            for (auto& txn_id : finished_txn) {
                ss << txn_id << ",";
            }
            LOG(INFO) << "publish version finished transactions: [" << ss.str() << "]";
            finished_txn.clear();
        }
    }
    return nullptr;
}

void* ReportTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportTaskWorkerPool*)arg_this;

    TReportRequest request;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(1);
            continue;
        }
        auto tasks = count_all_tasks();
        request.__set_tasks(tasks);
        request.__set_backend(BackendOptions::get_localBackend());

        StarRocksMetrics::instance()->report_task_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_task_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report task to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }

        nap_sleep(config::report_task_interval_seconds,
                  [worker_pool_this] { return worker_pool_this->_stopped.load(); });
    }

    return nullptr;
}

void* ReportDiskStateTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportDiskStateTaskWorkerPool*)arg_this;

    TReportRequest request;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }
        std::vector<DataDirInfo> data_dir_infos;
        StorageEngine::instance()->get_all_data_dir_info(&data_dir_infos, true /* update */);

        std::map<std::string, TDisk> disks;
        for (auto& root_path_info : data_dir_infos) {
            TDisk disk;
            disk.__set_root_path(root_path_info.path);
            disk.__set_path_hash(root_path_info.path_hash);
            disk.__set_storage_medium(root_path_info.storage_medium);
            disk.__set_disk_total_capacity(root_path_info.disk_capacity);
            disk.__set_data_used_capacity(root_path_info.data_used_capacity);
            disk.__set_disk_available_capacity(root_path_info.available);
            disk.__set_used(root_path_info.is_used);
            disks[root_path_info.path] = disk;

            StarRocksMetrics::instance()->disks_total_capacity.set_metric(root_path_info.path,
                                                                          root_path_info.disk_capacity);
            StarRocksMetrics::instance()->disks_avail_capacity.set_metric(root_path_info.path,
                                                                          root_path_info.available);
            StarRocksMetrics::instance()->disks_data_used_capacity.set_metric(root_path_info.path,
                                                                              root_path_info.data_used_capacity);
            StarRocksMetrics::instance()->disks_state.set_metric(root_path_info.path, root_path_info.is_used ? 1L : 0L);
        }
        request.__set_disks(disks);
        request.__set_backend(BackendOptions::get_localBackend());

        StarRocksMetrics::instance()->report_disk_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_disk_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report disk state to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }

        // wait for notifying until timeout
        wait_for_disk_report_notify([&] { return worker_pool_this->_stopped.load(); });
    }

    return nullptr;
}

void* ReportOlapTableTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportOlapTableTaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__isset.tablets = true;
    AgentStatus status = STARROCKS_SUCCESS;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }
        request.tablets.clear();

        int64_t report_version = g_report_version.load(std::memory_order_relaxed);
        request.__set_report_version(report_version);
        Status st_report = StorageEngine::instance()->tablet_manager()->report_all_tablets_info(&request.tablets);
        if (!st_report.ok()) {
            LOG(WARNING) << "Fail to report all tablets info, err=" << st_report.to_string();
            // wait for notifying until timeout
            wait_for_tablet_report_notify([&] { return worker_pool_this->_stopped.load(); });
            continue;
        }
        int64_t max_compaction_score =
                std::max(StarRocksMetrics::instance()->tablet_cumulative_max_compaction_score.value(),
                         StarRocksMetrics::instance()->tablet_base_max_compaction_score.value());
        request.__set_tablet_max_compaction_score(max_compaction_score);
        request.__set_backend(BackendOptions::get_localBackend());

        TMasterResult result;
        status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_all_tablets_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report olap table state to " << master_address.hostname << ":"
                         << master_address.port << ", err=" << status;
        } else {
            VLOG(1) << "Report tablets successfully, report version: " << report_version;
        }

        // wait for notifying until timeout
        wait_for_tablet_report_notify([&] { return worker_pool_this->_stopped.load(); });
    }

    return nullptr;
}

std::atomic<bool> ReportOlapTableTaskWorkerPool::_regular_report_stopped(false);

void* ReportWorkgroupTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportWorkgroupTaskWorkerPool*)arg_this;

    TReportRequest request;
    AgentStatus status = STARROCKS_SUCCESS;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }

        StarRocksMetrics::instance()->report_workgroup_requests_total.increment(1);
        request.__set_report_version(g_report_version.load(std::memory_order_relaxed));
        auto workgroups = ExecEnv::GetInstance()->workgroup_manager()->list_workgroups();
        request.__set_active_workgroups(workgroups);
        request.__set_backend(BackendOptions::get_localBackend());
        TMasterResult result;
        status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_workgroup_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report workgroup to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }
        if (result.__isset.workgroup_ops) {
            ExecEnv::GetInstance()->workgroup_manager()->apply(result.workgroup_ops);
        }
        nap_sleep(config::report_workgroup_interval_seconds,
                  [worker_pool_this] { return worker_pool_this->_stopped.load(); });
    }

    return nullptr;
}

void* ReportResourceUsageTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportResourceUsageTaskWorkerPool*)arg_this;

    TReportRequest request;
    AgentStatus status = STARROCKS_SUCCESS;

    ResourceGroupUsageRecorder group_usage_recorder;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }

        StarRocksMetrics::instance()->report_resource_usage_requests_total.increment(1);
        request.__set_backend(BackendOptions::get_localBackend());
        request.__set_report_version(g_report_version.load(std::memory_order_relaxed));

        TResourceUsage resource_usage;
        resource_usage.__set_num_running_queries(ExecEnv::GetInstance()->query_context_mgr()->size());
        resource_usage.__set_mem_used_bytes(GlobalEnv::GetInstance()->process_mem_tracker()->consumption());
        resource_usage.__set_mem_limit_bytes(GlobalEnv::GetInstance()->process_mem_tracker()->limit());
        worker_pool_this->_cpu_usage_recorder.update_interval();
        resource_usage.__set_cpu_used_permille(worker_pool_this->_cpu_usage_recorder.cpu_used_permille());

        resource_usage.__set_group_usages(group_usage_recorder.get_resource_group_usages());

        request.__set_resource_usage(std::move(resource_usage));
        TMasterResult result;
        status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_resource_usage_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report resource_usage to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(config::report_resource_usage_interval_ms));
    }

    return nullptr;
}

void* ReportDataCacheMetricsTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    const auto* worker_pool_this = static_cast<ReportDataCacheMetricsTaskWorkerPool*>(arg_this);

    TReportRequest request;
    AgentStatus status = STARROCKS_SUCCESS;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }

        StarRocksMetrics::instance()->report_datacache_metrics_requests_total.increment(1);
        request.__set_backend(BackendOptions::get_localBackend());
        request.__set_report_version(g_report_version.load(std::memory_order_relaxed));

        TDataCacheMetrics t_metrics{};
        const LocalCacheEngine* cache = DataCache::GetInstance()->local_cache();
        if (cache != nullptr && cache->is_initialized()) {
            const auto metrics = cache->cache_metrics();
            DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
        } else {
            t_metrics.__set_status(TDataCacheStatus::DISABLED);
        }

        request.__set_datacache_metrics(t_metrics);

        TMasterResult result;
        status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_datacache_metrics_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report resource_usage to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }
        size_t sleep_secs = config::report_datacache_metrics_interval_ms / 1000;
        nap_sleep(sleep_secs, [&]() { return worker_pool_this->_stopped.load(); });
    }

    return nullptr;
}

AgentStatus TaskWorkerPoolBase::get_tablet_info(TTabletId tablet_id, TSchemaHash schema_hash, int64_t signature,
                                                TTabletInfo* tablet_info) {
    AgentStatus status = STARROCKS_SUCCESS;

    tablet_info->__set_tablet_id(tablet_id);
    tablet_info->__set_schema_hash(schema_hash);
    Status st = StorageEngine::instance()->tablet_manager()->report_tablet_info(tablet_info);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to get tablet info, status=" << st.to_string() << " signature=" << signature;
        status = STARROCKS_ERROR;
    }
    return status;
}

template class TaskWorkerPool<PushReqAgentTaskRequest>;
template class TaskWorkerPool<PublishVersionAgentTaskRequest>;
template class TaskWorkerPool<AgentTaskRequestWithoutReqBody>;

} // namespace starrocks
