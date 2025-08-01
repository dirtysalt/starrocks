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

#pragma once

#include <bthread/mutex.h>

#include <algorithm>
#include <atomic>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>

#include "column/chunk.h"
#include "common/compiler_util.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/BackendService.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_mgr_fwd.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_state.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"
#include "util/internal_service_recoverable_stub.h"
#include "util/phmap/phmap.h"

namespace starrocks::pipeline {

using PTransmitChunkParamsPtr = std::shared_ptr<PTransmitChunkParams>;
struct ClosureContext {
    TUniqueId instance_id;
    int64_t sequence;
    int64_t send_timestamp;
};

struct TransmitChunkInfo {
    // For BUCKET_SHUFFLE_HASH_PARTITIONED, multiple channels may be related to
    // a same exchange source fragment instance, so we should use fragment_instance_id
    // of the destination as the key of destination instead of channel_id.
    TUniqueId fragment_instance_id;
    std::shared_ptr<PInternalService_RecoverableStub> brpc_stub;
    PTransmitChunkParamsPtr params;
    ChunkPassThroughVectorPtr pass_through_chunks;
    DataStreamMgr* stream_mgr;
    butil::IOBuf attachment;
    int64_t physical_bytes;
    const TNetworkAddress brpc_addr;
};

// TimeTrace is introduced to estimate time more accurately.
// For every update
// 1. times will be increased by 1.
// 2. sample time will be accumulated to accumulated_time.
// 3. sample concurrency will be accumulated to accumulated_concurrency.
// So we can get the average time of each direction by
// `average_concurrency = accumulated_concurrency / times`
// `average_time = accumulated_time / average_concurrency`
struct TimeTrace {
    int32_t times = 0;
    int64_t accumulated_time = 0;
    int32_t accumulated_concurrency = 0;

    void update(int64_t time, int32_t concurrency) {
        times++;
        accumulated_time += time;
        accumulated_concurrency += concurrency;
    }
};

class SinkBuffer {
public:
    SinkBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations,
               bool is_dest_merge);
    ~SinkBuffer();

    Status add_request(TransmitChunkInfo& request);
    bool is_full() const;

    void set_finishing();
    bool is_finished() const;

    // Add counters to the given profile
    void update_profile(RuntimeProfile* profile);

    // When all the ExchangeSinkOperator shared this SinkBuffer are cancelled,
    // the rest chunk request and EOS request needn't be sent anymore.
    void cancel_one_sinker(RuntimeState* const state);

    void incr_sinker(RuntimeState* state);

    void attach_observer(RuntimeState* state, PipelineObserver* observer) { _observable.add_observer(state, observer); }
    void notify_observers() { _observable.notify_sink_observers(); }
    auto defer_notify() {
        return DeferOp([this]() {
            _observable.notify_sink_observers();
            if (bthread_self()) {
                CHECK(tls_thread_status.mem_tracker() == GlobalEnv::GetInstance()->process_mem_tracker());
            }
        });
    }

    // Similar to defer_notify, but only attempts to notify all observers when sink buffer is finished
    auto finishing_defer() {
        return DeferOp([this]() {
            if (is_finished()) {
                this->defer_notify();
            }
        });
    }

    int64_t get_sent_bytes() const { return _bytes_sent; }

private:
    using Mutex = bthread::Mutex;

    void _update_network_time(const TUniqueId& instance_id, const int64_t send_timestamp,
                              const int64_t receiver_post_process_time);
    // Update the discontinuous acked window, here are the invariants:
    // all acks received with sequence from [0, _max_continuous_acked_seqs[x]]
    // not all the acks received with sequence from [_max_continuous_acked_seqs[x]+1, _request_seqs[x]]
    // _discontinuous_acked_seqs[x] stored the received discontinuous acks
    void _process_send_window(const TUniqueId& instance_id, const int64_t sequence);

    Status _try_to_send_local(const TUniqueId& instance_id, const std::function<void()>& pre_works);

    // Try to send rpc if buffer is not empty and channel is not busy
    // And we need to put this function and other extra works(pre_works) together as an atomic operation
    Status _try_to_send_rpc(const TUniqueId& instance_id, const std::function<void()>& pre_works);

    // send by rpc or http
    Status _send_rpc(DisposableClosure<PTransmitChunkResult, ClosureContext>* closure, const TransmitChunkInfo& req);

    // Roughly estimate network time which is defined as the time between sending a and receiving a packet,
    // and the processing time of both sides are excluded
    // For each destination, we may send multiply packages at the same time, and the time is
    // related to the degree of concurrency, so the network_time will be calculated as
    // `accumulated_network_time / average_concurrency`
    // And we just pick the maximum accumulated_network_time among all destination
    int64_t _network_time();

    void incr_sent_bytes(int64_t sent_bytes) {
        _bytes_sent += sent_bytes;
        _delta_bytes_sent += sent_bytes;
    }

    FragmentContext* _fragment_ctx;
    MemTracker* const _mem_tracker;
    const int32_t _brpc_timeout_ms;
    const bool _is_dest_merge;

    /// Taking into account of efficiency, all the following maps
    /// use int64_t as key, which is the field type of TUniqueId::lo
    /// because TUniqueId::hi is exactly the same in one query

    struct SinkContext {
        // num eos per instance
        int64_t num_sinker;
        int64_t request_seq;
        // Considering the following situation
        // Sending request 1, 2, 3 in order with one possible order of response 1, 3, 2,
        // and field transformation are as following
        //      a. receive response-1, _max_continuous_acked_seqs[x]->1, _discontinuous_acked_seqs[x]->()
        //      b. receive response-3, _max_continuous_acked_seqs[x]->1, _discontinuous_acked_seqs[x]->(3)
        //      c. receive response-2, _max_continuous_acked_seqs[x]->3, _discontinuous_acked_seqs[x]->()
        int64_t max_continuous_acked_seqs;
        std::unordered_set<int64_t> discontinuous_acked_seqs;
        // The request needs the reference to the allocated finst id,
        // so cache finst id for each dest fragment instance.
        PUniqueId finst_id;
        std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>> buffer;
        Mutex request_mutex;

        std::atomic_size_t num_finished_rpcs;
        std::atomic_size_t num_in_flight_rpcs;
        TimeTrace network_time;

        Mutex mutex;

        TNetworkAddress dest_addrs;

        std::atomic_bool pass_through_blocked;

        // currently only used for local send. Record the id of the thread that sent it.
        std::thread::id owner_id{};
    };
    phmap::flat_hash_map<int64_t, std::unique_ptr<SinkContext>, StdHash<int64_t>> _sink_ctxs;
    SinkContext& sink_ctx(int64_t instance_id) { return *_sink_ctxs[instance_id]; }

    std::atomic<int32_t> _total_in_flight_rpc = 0;
    std::atomic<int32_t> _num_uncancelled_sinkers = 0;
    std::atomic<int32_t> _num_remaining_eos = 0;

    // True means that SinkBuffer needn't input chunk and send chunk anymore,
    // but there may be still in-flight RPC running.
    // It becomes true, when all sinkers have sent EOS, or been set_finished/cancelled, or RPC has returned error.
    // Unfortunately, _is_finishing itself cannot guarantee that no brpc process will trigger if entering finishing stage,
    // considering the following situations(events order by time)
    //      time1(thread A): _try_to_send_rpc check _is_finishing which returns false
    //      time2(thread B): set _is_finishing to true
    //      time3(thread A): _try_to_send_rpc trigger brpc process
    // So _num_sending_rpc is introduced to solve this problem by providing extra information
    // of how many threads are calling _try_to_send_rpc
    std::atomic<bool> _is_finishing = false;
    std::atomic<int32_t> _num_sending = 0;

    std::atomic<int64_t> _rpc_count = 0;
    std::atomic<int64_t> _rpc_cumulative_time = 0;

    // RuntimeProfile counters
    std::atomic<int64_t> _bytes_enqueued = 0;
    std::atomic<int64_t> _request_enqueued = 0;
    std::atomic<int64_t> _bytes_sent = 0;
    std::atomic<int64_t> _request_sent = 0;

    std::atomic<int64_t> _delta_bytes_sent = 0;

    int64_t _pending_timestamp = -1;
    mutable std::atomic<int64_t> _last_full_timestamp = -1;
    mutable std::atomic<int64_t> _full_time = 0;

    // These two fields are used to calculate the overthroughput
    // Non-atomic type is enough because the concurrency inconsistency is acceptable
    int64_t _first_send_time = -1;
    int64_t _last_receive_time = -1;
    int64_t _rpc_http_min_size = 0;

    std::atomic<int64_t> _request_sequence = 0;
    int64_t _sent_audit_stats_frequency = 1;
    int64_t _sent_audit_stats_frequency_upper_limit = 64;

    Observable _observable;
};

} // namespace starrocks::pipeline
