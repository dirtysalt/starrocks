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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/runtime_state.h

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

#pragma once

#include <atomic>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "cache/block_cache/block_cache.h"
#include "cache/datacache_utils.h"
#include "cctz/time_zone.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/global_dict/parser.h"
#include "runtime/global_dict/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/debug_action.h"
#include "util/logging.h"
#include "util/phmap/phmap.h"
#include "util/runtime_profile.h"

namespace starrocks {

class DescriptorTbl;
class ObjectPool;
class Status;
class ExecEnv;
class Expr;
class DateTimeValue;
class MemTracker;
class DataStreamRecvr;
class ResultBufferMgr;
class LoadErrorHub;
class RowDescriptor;
class RuntimeFilterPort;
class QueryStatistics;
class QueryStatisticsRecvr;
using BroadcastJoinRightOffsprings = std::unordered_set<int32_t>;
namespace pipeline {
class QueryContext;
}

#define EXTRACE_SPILL_PARAM(query_option, spill_option, var) \
    spill_option.has_value() ? spill_option->var : query_option.var

constexpr int64_t kRpcHttpMinSize = ((1L << 31) - (1L << 10));

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
public:
    // for ut only
    RuntimeState() = default;
    // for ut only
    RuntimeState(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    RuntimeState(const TUniqueId& query_id, const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // RuntimeState for executing expr in fe-support.
    explicit RuntimeState(const TQueryGlobals& query_globals);

    explicit RuntimeState(ExecEnv* exec_env);

    // Empty d'tor to avoid issues with std::unique_ptr.
    ~RuntimeState();

    // Set up four-level hierarchy of mem trackers: process, query, fragment instance.
    // The instance tracker is tied to our profile.
    // Specific parts of the fragment (i.e. exec nodes, sinks, data stream senders, etc)
    // will add a fourth level when they are initialized.
    // This function also initializes a user function mem tracker (in the fourth level).
    void init_mem_trackers(const TUniqueId& query_id, MemTracker* parent = nullptr);

    void init_mem_trackers(const std::shared_ptr<MemTracker>& query_mem_tracker);

    // for ut only
    void init_instance_mem_tracker();

    const TQueryOptions& query_options() const { return _query_options; }
    ObjectPool* obj_pool() const { return _obj_pool.get(); }
    ObjectPool* global_obj_pool() const;
    void set_query_ctx(pipeline::QueryContext* ctx) { _query_ctx = ctx; }
    pipeline::QueryContext* query_ctx() { return _query_ctx; }
    pipeline::FragmentContext* fragment_ctx() { return _fragment_ctx; }
    void set_fragment_ctx(pipeline::FragmentContext* fragment_ctx) { _fragment_ctx = fragment_ctx; }
    const DescriptorTbl& desc_tbl() const { return *_desc_tbl; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) { _desc_tbl = desc_tbl; }
    int chunk_size() const { return _query_options.batch_size; }
    void set_chunk_size(int chunk_size) { _query_options.batch_size = chunk_size; }
    bool abort_on_default_limit_exceeded() const { return _query_options.abort_on_default_limit_exceeded; }
    int64_t timestamp_ms() const { return _timestamp_us / 1000; }
    int64_t timestamp_us() const { return _timestamp_us; }
    const std::string& timezone() const { return _timezone; }
    const cctz::time_zone& timezone_obj() const { return _timezone_obj; }
    bool set_timezone(const std::string& tz);
    const std::string& user() const { return _user; }
    const std::string& last_query_id() const { return _last_query_id; }
    const TUniqueId& query_id() const { return _query_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    ExecEnv* exec_env() { return _exec_env; }
    MemTracker* instance_mem_tracker() { return _instance_mem_tracker.get(); }
    MemPool* instance_mem_pool() { return _instance_mem_pool.get(); }
    std::shared_ptr<MemTracker> query_mem_tracker_ptr() { return _query_mem_tracker; }
    void set_query_mem_tracker(const std::shared_ptr<MemTracker>& query_mem_tracker) {
        _query_mem_tracker = query_mem_tracker;
    }
    const std::shared_ptr<MemTracker>& query_mem_tracker_ptr() const { return _query_mem_tracker; }
    std::shared_ptr<MemTracker> instance_mem_tracker_ptr() { return _instance_mem_tracker; }
    RuntimeFilterPort* runtime_filter_port() { return _runtime_filter_port; }
    const std::atomic<bool>& cancelled_ref() const { return _is_cancelled; }

    void set_fragment_root_id(PlanNodeId id) {
        DCHECK(_root_node_id == -1) << "Should not set this twice.";
        _root_node_id = id;
    }

    // Returns runtime state profile
    RuntimeProfile* runtime_profile() { return _profile.get(); }
    std::shared_ptr<RuntimeProfile> runtime_profile_ptr() { return _profile; }

    RuntimeProfile* load_channel_profile() { return _load_channel_profile.get(); }
    std::shared_ptr<RuntimeProfile> load_channel_profile_ptr() { return _load_channel_profile; }

    Status query_status() {
        std::lock_guard<std::mutex> l(_process_status_lock);
        return _process_status;
    };

    bool is_cancelled() const { return _is_cancelled.load(std::memory_order_acquire); }
    void set_is_cancelled(bool v) { _is_cancelled.store(v, std::memory_order_release); }

    void set_be_number(int be_number) { _be_number = be_number; }
    int be_number() const { return _be_number; }

    // Sets _process_status with err_msg if no error has been set yet.
    void set_process_status(const std::string& err_msg) {
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (!_process_status.ok()) {
            return;
        }
        _process_status = Status::InternalError(err_msg);
    }

    void set_process_status(const Status& status) {
        if (status.ok()) {
            return;
        }
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (!_process_status.ok()) {
            return;
        }
        _process_status = status;
    }

    // Sets query_status_ to MEM_LIMIT_EXCEEDED and logs all the registered trackers.
    // Subsequent calls to this will be no-ops. Returns query_status_.
    // If 'failed_allocation_size' is not 0, then it is the size of the allocation (in
    // bytes) that would have exceeded the limit allocated for 'tracker'.
    // This value and tracker are only used for error reporting.
    // If 'msg' is not empty, it will be appended to query_status_ in addition to the
    // generic "Memory limit exceeded" error.
    Status set_mem_limit_exceeded(MemTracker* tracker = nullptr, int64_t failed_allocation_size = 0,
                                  std::string_view msg = {});

    Status set_mem_limit_exceeded(std::string_view msg) { return set_mem_limit_exceeded(nullptr, 0, msg); }

    // Returns a non-OK status if query execution should stop (e.g., the query was cancelled
    // or a mem limit was exceeded). Exec nodes should check this periodically so execution
    // doesn't continue if the query terminates abnormally.
    Status check_query_state(const std::string& msg);

    Status check_mem_limit(const std::string& msg);

    std::vector<std::string>& output_files() { return _output_files; }

    const std::vector<std::string>& export_output_files() const { return _export_output_files; }

    void add_export_output_file(const std::string& file) { _export_output_files.push_back(file); }

    void set_txn_id(int64_t txn_id) { _txn_id = txn_id; }

    int64_t load_job_id() const { return _txn_id; }

    void set_db(const std::string& db) { _db = db; }

    const std::string& db() const { return _db; }

    void set_load_label(const std::string& label) { _load_label = label; }

    const std::string& load_label() const { return _load_label; }

    const std::string& get_error_log_file_path() const { return _error_log_file_path; }

    const std::string& get_rejected_record_file_path() const { return _rejected_record_file_path; }

    // is_summary is true, means we are going to write the summary line
    void append_error_msg_to_file(const std::string& line, const std::string& error_msg, bool is_summary = false);

    bool has_reached_max_error_msg_num(bool is_summary = false);

    Status create_rejected_record_file();

    bool enable_log_rejected_record() {
        return _query_options.log_rejected_record_num == -1 ||
               _query_options.log_rejected_record_num > _num_log_rejected_rows;
    }

    void append_rejected_record_to_file(const std::string& record, const std::string& error_msg,
                                        const std::string& source);

    int64_t num_bytes_load_from_source() const noexcept { return _num_bytes_load_from_source.load(); }

    int64_t num_rows_load_from_source() const noexcept { return _num_rows_load_total_from_source.load(); }

    int64_t num_bytes_load_sink() const noexcept { return _num_bytes_load_sink.load(); }

    int64_t num_rows_load_sink() const noexcept { return _num_rows_load_sink.load(); }

    int64_t num_rows_load_filtered() const noexcept { return _num_rows_load_filtered.load(); }

    int64_t num_rows_load_unselected() const noexcept { return _num_rows_load_unselected.load(); }

    int64_t num_bytes_scan_from_source() const noexcept { return _num_bytes_scan_from_source.load(); }

    void update_num_bytes_load_from_source(int64_t bytes_load) { _num_bytes_load_from_source.fetch_add(bytes_load); }

    void update_num_rows_load_from_source(int64_t num_rows) { _num_rows_load_total_from_source.fetch_add(num_rows); }

    void update_num_bytes_load_sink(int64_t bytes_load) { _num_bytes_load_sink.fetch_add(bytes_load); }

    void update_num_rows_load_sink(int64_t num_rows) { _num_rows_load_sink.fetch_add(num_rows); }

    void update_num_rows_load_filtered(int64_t num_rows) { _num_rows_load_filtered.fetch_add(num_rows); }

    void update_num_rows_load_unselected(int64_t num_rows) { _num_rows_load_unselected.fetch_add(num_rows); }

    void update_num_bytes_scan_from_source(int64_t scan_bytes) { _num_bytes_scan_from_source.fetch_add(scan_bytes); }

    void update_report_load_status(TReportExecStatusParams* load_params) {
        load_params->__set_loaded_rows(num_rows_load_sink());
        load_params->__set_sink_load_bytes(num_bytes_load_sink());
        load_params->__set_source_load_rows(num_rows_load_from_source());
        load_params->__set_source_load_bytes(num_bytes_load_from_source());
        load_params->__set_filtered_rows(num_rows_load_filtered());
        load_params->__set_unselected_rows(num_rows_load_unselected());
        load_params->__set_source_scan_bytes(num_bytes_scan_from_source());
        // Update datacache load metrics
        update_load_datacache_metrics(load_params);
    }

    void update_num_datacache_read_bytes(const int64_t read_bytes) {
        _num_datacache_read_bytes.fetch_add(read_bytes, std::memory_order_relaxed);
    }

    void update_num_datacache_read_time_ns(const int64_t read_time) {
        _num_datacache_read_time_ns.fetch_add(read_time, std::memory_order_relaxed);
    }

    void update_num_datacache_write_bytes(const int64_t write_bytes) {
        _num_datacache_write_bytes.fetch_add(write_bytes, std::memory_order_relaxed);
    }

    void update_num_datacache_write_time_ns(const int64_t write_time) {
        _num_datacache_write_time_ns.fetch_add(write_time, std::memory_order_relaxed);
    }

    void update_num_datacache_count(const int64_t count) {
        _num_datacache_count.fetch_add(count, std::memory_order_relaxed);
    }

    void update_load_datacache_metrics(TReportExecStatusParams* load_params) const;

    std::atomic_int64_t* mutable_total_spill_bytes();

    void set_per_fragment_instance_idx(int idx) { _per_fragment_instance_idx = idx; }

    int per_fragment_instance_idx() const { return _per_fragment_instance_idx; }

    void set_num_per_fragment_instances(int num_instances) { _num_per_fragment_instances = num_instances; }

    int num_per_fragment_instances() const { return _num_per_fragment_instances; }

    TSpillMode::type spill_mode() const { return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_mode); }
    int64_t spillable_operator_mask() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spillable_operator_mask);
    }

    bool enable_spill() const { return _query_options.enable_spill; }

    bool enable_hash_join_spill() const {
        return spillable_operator_mask() & (1LL << TSpillableOperatorType::HASH_JOIN);
    }

    bool enable_agg_spill() const { return spillable_operator_mask() & (1LL << TSpillableOperatorType::AGG); }
    bool enable_spill_partitionwise_agg() const { return enable_agg_spill() && spill_partitionwise_agg(); }
    int spill_partitionwise_agg_partition_num() const {
        if (_spill_options->spill_partitionwise_agg_partition_num <= 0) {
            return config::spill_init_partition;
        } else {
            return std::max(std::min(_spill_options->spill_partitionwise_agg_partition_num, 256), 4);
        }
    }
    bool enable_spill_partitionwise_agg_skew_elimination() const {
        return enable_agg_spill() && spill_partitionwise_agg_skew_elimination();
    }
    bool enable_agg_distint_spill() const {
        return spillable_operator_mask() & (1LL << TSpillableOperatorType::AGG_DISTINCT);
    }
    bool enable_sort_spill() const { return spillable_operator_mask() & (1LL << TSpillableOperatorType::SORT); }
    bool enable_nl_join_spill() const { return spillable_operator_mask() & (1LL << TSpillableOperatorType::NL_JOIN); }
    bool enable_multi_cast_local_exchange_spill() const {
        return spillable_operator_mask() & (1LL << TSpillableOperatorType::MULTI_CAST_LOCAL_EXCHANGE);
    }

    bool spill_partitionwise_agg() const { return _spill_options->spill_partitionwise_agg; }
    bool spill_partitionwise_agg_skew_elimination() const {
        return _spill_options->spill_partitionwise_agg_skew_elimination;
    }

    int32_t spill_mem_table_size() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_mem_table_size);
    }

    int32_t spill_mem_table_num() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_mem_table_num);
    }

    bool enable_agg_spill_preaggregation() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, enable_agg_spill_preaggregation);
    }

    double spill_mem_limit_threshold() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_mem_limit_threshold);
    }

    int64_t spill_operator_min_bytes() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_operator_min_bytes);
    }

    int64_t spill_operator_max_bytes() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_operator_max_bytes);
    }

    int64_t spill_revocable_max_bytes() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_revocable_max_bytes);
    }
    bool spill_enable_direct_io() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_enable_direct_io);
    }
    double spill_rand_ratio() const { return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_rand_ratio); }

    int32_t spill_encode_level() const {
        return EXTRACE_SPILL_PARAM(_query_options, _spill_options, spill_encode_level);
    }
    bool spill_enable_compaction() const {
        return _spill_options.has_value() ? _spill_options->spill_enable_compaction : false;
    }
    bool enable_spill_buffer_read() const {
        return _spill_options.has_value() ? _spill_options->enable_spill_buffer_read : false;
    }
    int64_t max_spill_read_buffer_bytes_per_driver() const {
        return _spill_options.has_value() ? _spill_options->max_spill_read_buffer_bytes_per_driver : INT64_MAX;
    }
    int64_t spill_hash_join_probe_op_max_bytes() const {
        return _spill_options.has_value() ? _spill_options->spill_hash_join_probe_op_max_bytes : 1LL << 31;
    }

    bool error_if_overflow() const {
        return _query_options.__isset.overflow_mode && _query_options.overflow_mode == TOverflowMode::REPORT_ERROR;
    }

    bool enable_hyperscan_vec() const {
        return _query_options.__isset.enable_hyperscan_vec && _query_options.enable_hyperscan_vec;
    }

    long column_view_concat_rows_limit() const {
        return _query_options.__isset.column_view_concat_rows_limit ? _query_options.column_view_concat_rows_limit
                                                                    : -1L;
    }

    long column_view_concat_bytes_limit() const {
        return _query_options.__isset.column_view_concat_bytes_limit ? _query_options.column_view_concat_bytes_limit
                                                                     : -1L;
    }

    bool enable_column_view() const {
        return column_view_concat_bytes_limit() > 0 || column_view_concat_rows_limit() > 0;
    }

    bool enable_hash_join_range_direct_mapping_opt() const {
        return _query_options.__isset.enable_hash_join_range_direct_mapping_opt &&
               _query_options.enable_hash_join_range_direct_mapping_opt;
    }

    const std::vector<TTabletCommitInfo>& tablet_commit_infos() const { return _tablet_commit_infos; }

    std::vector<TTabletCommitInfo>& tablet_commit_infos() { return _tablet_commit_infos; }

    void append_tablet_commit_infos(std::vector<TTabletCommitInfo>& commit_info) {
        std::lock_guard<std::mutex> l(_tablet_infos_lock);
        _tablet_commit_infos.insert(_tablet_commit_infos.end(), std::make_move_iterator(commit_info.begin()),
                                    std::make_move_iterator(commit_info.end()));
    }

    const std::vector<TTabletFailInfo>& tablet_fail_infos() const { return _tablet_fail_infos; }

    std::vector<TTabletFailInfo>& tablet_fail_infos() { return _tablet_fail_infos; }

    void append_tablet_fail_infos(const TTabletFailInfo& fail_info) {
        std::lock_guard<std::mutex> l(_tablet_infos_lock);
        _tablet_fail_infos.emplace_back(std::move(fail_info));
    }

    std::vector<TSinkCommitInfo>& sink_commit_infos() {
        std::lock_guard<std::mutex> l(_sink_commit_infos_lock);
        return _sink_commit_infos;
    }

    void add_sink_commit_info(const TSinkCommitInfo& sink_commit_info) {
        std::lock_guard<std::mutex> l(_sink_commit_infos_lock);
        _sink_commit_infos.emplace_back(std::move(sink_commit_info));
    }

    // get mem limit for load channel
    // if load mem limit is not set, or is zero, using query mem limit instead.
    int64_t get_load_mem_limit() const;

    const GlobalDictMaps& get_query_global_dict_map() const;
    // for query global dict
    GlobalDictMaps* mutable_query_global_dict_map();

    const GlobalDictMaps& get_load_global_dict_map() const;

    DictOptimizeParser* mutable_dict_optimize_parser();

    const phmap::flat_hash_map<uint32_t, int64_t>& load_dict_versions() { return _load_dict_versions; }

    using GlobalDictLists = std::vector<TGlobalDict>;
    Status init_query_global_dict(const GlobalDictLists& global_dict_list);
    Status init_load_global_dict(const GlobalDictLists& global_dict_list);

    Status init_query_global_dict_exprs(const std::map<int, TExpr>& exprs);

    void set_func_version(int func_version) { this->_func_version = func_version; }
    int func_version() const { return this->_func_version; }

    void set_enable_pipeline_engine(bool enable_pipeline_engine) { _enable_pipeline_engine = enable_pipeline_engine; }
    bool enable_pipeline_engine() const { return _enable_pipeline_engine; }

    std::shared_ptr<QueryStatisticsRecvr> query_recv();

    Status reset_epoch();

    int64_t get_rpc_http_min_size() {
        return _query_options.__isset.rpc_http_min_size ? _query_options.rpc_http_min_size : kRpcHttpMinSize;
    }

    bool use_page_cache();

    bool enable_collect_table_level_scan_stats() const {
        return _query_options.__isset.enable_collect_table_level_scan_stats &&
               _query_options.enable_collect_table_level_scan_stats;
    }

    bool enable_wait_dependent_event() const {
        return _query_options.__isset.enable_wait_dependent_event && _query_options.enable_wait_dependent_event;
    }

    bool is_jit_enabled() const;

    bool is_adaptive_jit() const { return _query_options.__isset.jit_level && _query_options.jit_level == 1; }

    void set_jit_level(const int level) { _query_options.__set_jit_level(level); }

    // CompilableExprType
    // arithmetic -> 2, except /, %
    // cast -> 4
    // case -> 8
    // cmp -> 16
    // logical -> 32
    // div -> 64
    // mod -> 128
    bool can_jit_expr(const int jit_label) {
        return (_query_options.jit_level == 1) || ((_query_options.jit_level & jit_label));
    }

    std::string_view get_sql_dialect() const { return _query_options.sql_dialect; }

    void set_non_broadcast_rf_ids(std::unordered_set<int32_t>&& filter_ids) {
        this->_non_broadcast_rf_ids = std::move(filter_ids);
    }

    const std::unordered_set<int32_t>& non_broadcast_rf_ids() const { return this->_non_broadcast_rf_ids; }

    void set_broadcast_join_right_offsprings(BroadcastJoinRightOffsprings&& broadcast_join_right_offsprings) {
        this->_broadcast_join_right_offsprings = std::move(broadcast_join_right_offsprings);
    }

    const BroadcastJoinRightOffsprings& broadcast_join_right_offsprings() const {
        return this->_broadcast_join_right_offsprings;
    }

    bool enable_event_scheduler() const { return _enable_event_scheduler; }
    void set_enable_event_scheduler(bool enable) { _enable_event_scheduler = enable; }

    bool enable_join_runtime_filter_pushdown() const {
        return _query_options.__isset.enable_join_runtime_filter_pushdown &&
               _query_options.enable_join_runtime_filter_pushdown;
    }

    bool enable_join_runtime_bitset_filter() const {
        return _query_options.__isset.enable_join_runtime_bitset_filter &&
               _query_options.enable_join_runtime_bitset_filter;
    }

    bool lower_upper_support_utf8() const {
        return _query_options.__isset.lower_upper_support_utf8 && _query_options.lower_upper_support_utf8;
    }

    DebugActionMgr& debug_action_mgr() { return _debug_action_mgr; }

private:
    // Set per-query state.
    void _init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
               const TQueryGlobals& query_globals, ExecEnv* exec_env);

    Status create_error_log_file();

    Status _build_global_dict(const GlobalDictLists& global_dict_list, GlobalDictMaps* result,
                              phmap::flat_hash_map<uint32_t, int64_t>* version);

    // put runtime state before _obj_pool, so that it will be deconstructed after
    // _obj_pool. Because some object in _obj_pool will use profile when deconstructing.
    std::shared_ptr<RuntimeProfile> _profile;
    std::shared_ptr<RuntimeProfile> _load_channel_profile;

    // An aggregation function may have multiple versions of implementation, func_version determines the chosen version.
    int _func_version = 0;

    DescriptorTbl* _desc_tbl = nullptr;

    // Lock protecting _error_log and _unreported_error_idx
    std::mutex _error_log_lock;

    std::mutex _rejected_record_lock;
    std::string _rejected_record_file_path;
    std::unique_ptr<std::ofstream> _rejected_record_file;

    // _error_log[_unreported_error_idx+] has been not reported to the coordinator.
    int _unreported_error_idx;

    // Username of user that is executing the query to which this RuntimeState belongs.
    std::string _user;

    //Query-global timestamp_ms
    int64_t _timestamp_us = 0;
    std::string _timezone;
    cctz::time_zone _timezone_obj;

    std::string _last_query_id;
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    TQueryOptions _query_options;
    ExecEnv* _exec_env = nullptr;

    // MemTracker that is shared by all fragment instances running on this host.
    // The query mem tracker must be released after the _instance_mem_tracker.
    std::shared_ptr<MemTracker> _query_mem_tracker;

    // Memory usage of this fragment instance
    std::shared_ptr<MemTracker> _instance_mem_tracker;

    std::shared_ptr<ObjectPool> _obj_pool;

    // if true, execution should stop with a CANCELLED status
    std::atomic<bool> _is_cancelled{false};

    int _per_fragment_instance_idx;
    int _num_per_fragment_instances = 0;

    // used as send id
    int _be_number = 0;

    // Non-OK if an error has occurred and query execution should abort. Used only for
    // asynchronously reporting such errors (e.g., when a UDF reports an error), so this
    // will not necessarily be set in all error cases.
    std::mutex _process_status_lock;
    Status _process_status;
    std::unique_ptr<MemPool> _instance_mem_pool;

    // This is the node id of the root node for this plan fragment. This is used as the
    // hash seed and has two useful properties:
    // 1) It is the same for all exec nodes in a fragment, so the resulting hash values
    // can be shared (i.e. for _slot_bitmap_filters).
    // 2) It is different between different fragments, so we do not run into hash
    // collisions after data partitioning (across fragments). See IMPALA-219 for more
    // details.
    PlanNodeId _root_node_id = -1;

    // put here to collect files??
    std::vector<std::string> _output_files;

    // here we separate number counter of rows/bytes load from source node and sink node,
    // num_rows/bytes_load_from_source is mainly used for reporting the progress of broker load and insert into
    // num_rows/bytes_load_from_sink is mainly used for recording how many rows are loaded into tablet sink
    std::atomic<int64_t> _num_rows_load_total_from_source{
            0};                                          // total rows load from source node (file scan node, olap scan
                                                         // node)
    std::atomic<int64_t> _num_bytes_load_from_source{0}; // total bytes load from source node (file scan node, olap scan
                                                         // node)

    std::atomic<int64_t> _num_rows_load_sink{0};  // total rows sink to storage
    std::atomic<int64_t> _num_bytes_load_sink{0}; // total bytes sink to storage

    std::atomic<int64_t> _num_rows_load_filtered{0};     // unqualified rows
    std::atomic<int64_t> _num_rows_load_unselected{0};   // rows filtered by predicates
    std::atomic<int64_t> _num_bytes_scan_from_source{0}; // total bytes scan from source node

    // datacache select metrics
    std::atomic<int64_t> _num_datacache_read_bytes{0};
    std::atomic<int64_t> _num_datacache_read_time_ns{0};
    std::atomic<int64_t> _num_datacache_write_bytes{0};
    std::atomic<int64_t> _num_datacache_write_time_ns{0};
    std::atomic<int64_t> _num_datacache_count{0};

    std::atomic<int64_t> _num_print_error_rows{0};
    std::atomic<int64_t> _num_log_rejected_rows{0}; // rejected rows

    std::vector<std::string> _export_output_files;

    int64_t _txn_id = 0;
    std::string _load_label;
    std::string _db;

    std::string _error_log_file_path;
    std::ofstream* _error_log_file = nullptr; // error file path, absolute path
    std::mutex _tablet_infos_lock;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;
    std::vector<TTabletFailInfo> _tablet_fail_infos;

    std::mutex _sink_commit_infos_lock;
    std::vector<TSinkCommitInfo> _sink_commit_infos;

    // prohibit copies
    RuntimeState(const RuntimeState&) = delete;

    RuntimeFilterPort* _runtime_filter_port = nullptr;

    GlobalDictMaps _query_global_dicts;
    GlobalDictMaps _load_global_dicts;
    phmap::flat_hash_map<uint32_t, int64_t> _load_dict_versions;
    DictOptimizeParser _dict_optimize_parser;

    pipeline::QueryContext* _query_ctx = nullptr;
    pipeline::FragmentContext* _fragment_ctx = nullptr;

    bool _enable_pipeline_engine = false;

    std::unordered_set<int32_t> _non_broadcast_rf_ids;
    BroadcastJoinRightOffsprings _broadcast_join_right_offsprings;

    std::optional<TSpillOptions> _spill_options;

    DebugActionMgr _debug_action_mgr;

    bool _enable_event_scheduler = false;
};

#define RETURN_IF_LIMIT_EXCEEDED(state, msg)                                                \
    do {                                                                                    \
        MemTracker* tracker = state->instance_mem_tracker()->find_limit_exceeded_tracker(); \
        if (tracker != nullptr) {                                                           \
            return Status::MemoryLimitExceeded(tracker->err_msg(msg, state));               \
        }                                                                                   \
    } while (false)

#define RETURN_IF_CANCELLED(state)                                                       \
    do {                                                                                 \
        if (UNLIKELY((state)->is_cancelled()))                                           \
            return Status::Cancelled("Cancelled because of runtime state is cancelled"); \
    } while (false)

} // namespace starrocks
