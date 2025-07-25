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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/system_metrics.cpp

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

#include "util/system_metrics.h"

#include <runtime/exec_env.h>
#ifdef WITH_TENANN
#include <tenann/index/index_cache.h>
#endif

#include <cstdio>
#include <memory>

#include "cache/datacache.h"
#ifdef USE_STAROS
#include "fslib/star_cache_handler.h"
#endif
#include "cache/object_cache/page_cache.h"
#include "gutil/strings/split.h" // for string split
#include "gutil/strtoint.h"      //  for atoi64
#include "io/io_profiler.h"
#include "jemalloc/jemalloc.h"
#include "runtime/runtime_filter_worker.h"
#include "util/metrics.h"

namespace starrocks {

const char* const SystemMetrics::_s_hook_name = "system_metrics";

// /proc/stat: http://www.linuxhowtos.org/System/procstat.htm
class CpuMetrics {
public:
    static constexpr int cpu_num_metrics = 10;
    std::unique_ptr<IntAtomicCounter> metrics[cpu_num_metrics] = {
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT),
            std::make_unique<IntAtomicCounter>(MetricUnit::PERCENT)};
    static const char* const cpu_metrics[cpu_num_metrics];
};

const char* const CpuMetrics::cpu_metrics[] = {"user", "nice",     "system", "idle",  "iowait",
                                               "irq",  "soft_irq", "steal",  "guest", "guest_nice"};

class DiskMetrics {
public:
    METRIC_DEFINE_INT_ATOMIC_COUNTER(reads_completed, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(bytes_read, MetricUnit::BYTES);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(read_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(writes_completed, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(bytes_written, MetricUnit::BYTES);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(write_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(io_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(io_time_weigthed, MetricUnit::MILLISECONDS);
};

class NetMetrics {
public:
    METRIC_DEFINE_INT_ATOMIC_COUNTER(receive_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(receive_packets, MetricUnit::PACKETS);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(send_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(send_packets, MetricUnit::PACKETS);
};

// metrics read from /proc/net/snmp
class SnmpMetrics {
public:
    // The number of all problematic TCP packets received
    METRIC_DEFINE_INT_ATOMIC_COUNTER(tcp_in_errs, MetricUnit::NOUNIT);
    // All TCP packets retransmitted
    METRIC_DEFINE_INT_ATOMIC_COUNTER(tcp_retrans_segs, MetricUnit::NOUNIT);
    // All received TCP packets
    METRIC_DEFINE_INT_ATOMIC_COUNTER(tcp_in_segs, MetricUnit::NOUNIT);
    // All send TCP packets with RST mark
    METRIC_DEFINE_INT_ATOMIC_COUNTER(tcp_out_segs, MetricUnit::NOUNIT);
};

class FileDescriptorMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(fd_num_limit, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(fd_num_used, MetricUnit::NOUNIT);
};

class QueryCacheMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(query_cache_capacity, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(query_cache_usage, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(query_cache_usage_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(query_cache_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(query_cache_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(query_cache_hit_ratio, MetricUnit::PERCENT);
};

class VectorIndexCacheMetrics {
    friend class SystemMetrics;

public:
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_capacity, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_usage, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_usage_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_hit_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_dynamic_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_dynamic_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_dynamic_hit_ratio, MetricUnit::PERCENT);

private:
    int _previous_lookup_count;
    int _previous_hit_count;
};

class RuntimeFilterMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(runtime_filter_events_in_queue, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(runtime_filter_bytes_in_queue, MetricUnit::BYTES);
};

SystemMetrics::SystemMetrics() = default;

SystemMetrics::~SystemMetrics() {
    // we must deregister us from registry
    if (_registry != nullptr) {
        _registry->deregister_hook(_s_hook_name);
        _registry = nullptr;
    }
    for (auto& it : _disk_metrics) {
        delete it.second;
    }
    for (auto& it : _net_metrics) {
        delete it.second;
    }
    if (_line_ptr != nullptr) {
        free(_line_ptr);
    }
    for (auto& it : _runtime_filter_metrics) {
        delete it.second;
    }
    for (auto& it : _io_metrics) {
        delete it;
    }
}

void SystemMetrics::install(MetricRegistry* registry, const std::set<std::string>& disk_devices,
                            const std::vector<std::string>& network_interfaces) {
    DCHECK(_registry == nullptr);
    if (!registry->register_hook(_s_hook_name, [this] { update(); })) {
        return;
    }
    _install_cpu_metrics(registry);
    _install_memory_metrics(registry);
    _install_disk_metrics(registry, disk_devices);
    _install_net_metrics(registry, network_interfaces);
    _install_fd_metrics(registry);
    _install_snmp_metrics(registry);
    _install_query_cache_metrics(registry);
    _install_runtime_filter_metrics(registry);
    _install_vector_index_cache_metrics(registry);
    _install_io_metrics(registry);
    _registry = registry;
}

void SystemMetrics::update() {
    _update_cpu_metrics();
    _update_memory_metrics();
    _update_disk_metrics();
    _update_net_metrics();
    _update_fd_metrics();
    _update_snmp_metrics();
    _update_query_cache_metrics();
    _update_runtime_filter_metrics();
    _update_vector_index_cache_metrics();
}

void SystemMetrics::_install_cpu_metrics(MetricRegistry* registry) {
    _cpu_metrics = std::make_unique<CpuMetrics>();

    for (int i = 0; i < CpuMetrics::cpu_num_metrics; ++i) {
        registry->register_metric("cpu", MetricLabels().add("mode", CpuMetrics::cpu_metrics[i]),
                                  _cpu_metrics->metrics[i].get());
    }
}

#ifdef BE_TEST
const char* k_ut_stat_path;      // NOLINT
const char* k_ut_diskstats_path; // NOLINT
const char* k_ut_net_dev_path;   // NOLINT
const char* k_ut_fd_path;        // NOLINT
const char* k_ut_net_snmp_path;  // NOLINT
#endif

void SystemMetrics::_update_cpu_metrics() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_stat_path, "r");
#else
    FILE* fp = fopen("/proc/stat", "r");
#endif
    if (fp == nullptr) {
        PLOG(WARNING) << "open /proc/stat failed";
        return;
    }

    if (getline(&_line_ptr, &_line_buf_size, fp) < 0) {
        PLOG(WARNING) << "getline failed";
        fclose(fp);
        return;
    }

    char cpu[16];
    int64_t values[CpuMetrics::cpu_num_metrics];
    memset(values, 0, sizeof(values));
    sscanf(_line_ptr,
           "%15s"
           " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
           " %" PRId64,
           cpu, &values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6], &values[7],
           &values[8], &values[9]);

    for (int i = 0; i < CpuMetrics::cpu_num_metrics; ++i) {
        _cpu_metrics->metrics[i]->set_value(values[i]);
    }

    fclose(fp);
}

void SystemMetrics::_install_memory_metrics(MetricRegistry* registry) {
    _memory_metrics = std::make_unique<MemoryMetrics>();
    registry->register_metric("jemalloc_allocated_bytes", &_memory_metrics->jemalloc_allocated_bytes);
    registry->register_metric("jemalloc_active_bytes", &_memory_metrics->jemalloc_active_bytes);
    registry->register_metric("jemalloc_metadata_bytes", &_memory_metrics->jemalloc_metadata_bytes);
    registry->register_metric("jemalloc_metadata_thp", &_memory_metrics->jemalloc_metadata_thp);
    registry->register_metric("jemalloc_resident_bytes", &_memory_metrics->jemalloc_resident_bytes);
    registry->register_metric("jemalloc_mapped_bytes", &_memory_metrics->jemalloc_mapped_bytes);
    registry->register_metric("jemalloc_retained_bytes", &_memory_metrics->jemalloc_retained_bytes);

    registry->register_metric("process_mem_bytes", &_memory_metrics->process_mem_bytes);
    registry->register_metric("query_mem_bytes", &_memory_metrics->query_mem_bytes);
    registry->register_metric("connector_scan_pool_mem_bytes", &_memory_metrics->connector_scan_pool_mem_bytes);
    registry->register_metric("load_mem_bytes", &_memory_metrics->load_mem_bytes);
    registry->register_metric("metadata_mem_bytes", &_memory_metrics->metadata_mem_bytes);
    registry->register_metric("tablet_metadata_mem_bytes", &_memory_metrics->tablet_metadata_mem_bytes);
    registry->register_metric("rowset_metadata_mem_bytes", &_memory_metrics->rowset_metadata_mem_bytes);
    registry->register_metric("segment_metadata_mem_bytes", &_memory_metrics->segment_metadata_mem_bytes);
    registry->register_metric("column_metadata_mem_bytes", &_memory_metrics->column_metadata_mem_bytes);
    registry->register_metric("tablet_schema_mem_bytes", &_memory_metrics->tablet_schema_mem_bytes);
    registry->register_metric("column_zonemap_index_mem_bytes", &_memory_metrics->column_zonemap_index_mem_bytes);
    registry->register_metric("ordinal_index_mem_bytes", &_memory_metrics->ordinal_index_mem_bytes);
    registry->register_metric("bitmap_index_mem_bytes", &_memory_metrics->bitmap_index_mem_bytes);
    registry->register_metric("bloom_filter_index_mem_bytes", &_memory_metrics->bloom_filter_index_mem_bytes);
    registry->register_metric("segment_zonemap_mem_bytes", &_memory_metrics->segment_zonemap_mem_bytes);
    registry->register_metric("short_key_index_mem_bytes", &_memory_metrics->short_key_index_mem_bytes);
    registry->register_metric("compaction_mem_bytes", &_memory_metrics->compaction_mem_bytes);
    registry->register_metric("schema_change_mem_bytes", &_memory_metrics->schema_change_mem_bytes);
    registry->register_metric("storage_page_cache_mem_bytes", &_memory_metrics->storage_page_cache_mem_bytes);
    registry->register_metric("jit_cache_mem_bytes", &_memory_metrics->jit_cache_mem_bytes);
    registry->register_metric("update_mem_bytes", &_memory_metrics->update_mem_bytes);
    registry->register_metric("clone_mem_bytes", &_memory_metrics->clone_mem_bytes);
    registry->register_metric("consistency_mem_bytes", &_memory_metrics->consistency_mem_bytes);
    registry->register_metric("datacache_mem_bytes", &_memory_metrics->datacache_mem_bytes);
}

void SystemMetrics::_update_datacache_mem_tracker() {
    // update datacache mem_tracker
    int64_t datacache_mem_bytes = 0;
    auto* datacache_mem_tracker = GlobalEnv::GetInstance()->datacache_mem_tracker();
    if (datacache_mem_tracker) {
        LocalCacheEngine* local_cache = DataCache::GetInstance()->local_cache();
        if (local_cache != nullptr && local_cache->is_initialized()) {
            auto datacache_metrics = local_cache->cache_metrics();
            datacache_mem_bytes = datacache_metrics.mem_used_bytes + datacache_metrics.meta_used_bytes;
        }
#ifdef USE_STAROS
        if (!config::datacache_unified_instance_enable) {
            datacache_mem_bytes += staros::starlet::fslib::star_cache_get_memory_usage();
        }
#endif
        datacache_mem_tracker->set(datacache_mem_bytes);
    }
}

void SystemMetrics::_update_pagecache_mem_tracker() {
    auto* pagecache_mem_tracker = GlobalEnv::GetInstance()->page_cache_mem_tracker();
    auto* page_cache = StoragePageCache::instance();
    if (pagecache_mem_tracker && page_cache != nullptr && page_cache->is_initialized()) {
        pagecache_mem_tracker->set(page_cache->memory_usage());
    }
}

void SystemMetrics::_update_memory_metrics() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    LOG(INFO) << "Memory tracking is not available with address sanitizer builds.";
#else
    size_t value = 0;
    // Update the statistics cached by mallctl.
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);
    sz = sizeof(size_t);
    if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_allocated_bytes.set_value(value);
    }
    if (je_mallctl("stats.active", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_active_bytes.set_value(value);
    }
    if (je_mallctl("stats.metadata", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_metadata_bytes.set_value(value);
    }
    if (je_mallctl("stats.metadata_thp", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_metadata_thp.set_value(value);
    }
    if (je_mallctl("stats.resident", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_resident_bytes.set_value(value);
    }
    if (je_mallctl("stats.mapped", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_mapped_bytes.set_value(value);
    }
    if (je_mallctl("stats.retained", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_retained_bytes.set_value(value);
    }
#endif

    _update_datacache_mem_tracker();
    _update_pagecache_mem_tracker();

#define SET_MEM_METRIC_VALUE(tracker, key)                                                  \
    if (GlobalEnv::GetInstance()->tracker() != nullptr) {                                   \
        _memory_metrics->key.set_value(GlobalEnv::GetInstance()->tracker()->consumption()); \
    }

    SET_MEM_METRIC_VALUE(process_mem_tracker, process_mem_bytes)
    SET_MEM_METRIC_VALUE(query_pool_mem_tracker, query_mem_bytes)
    SET_MEM_METRIC_VALUE(connector_scan_pool_mem_tracker, connector_scan_pool_mem_bytes)
    SET_MEM_METRIC_VALUE(load_mem_tracker, load_mem_bytes)
    SET_MEM_METRIC_VALUE(metadata_mem_tracker, metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(tablet_metadata_mem_tracker, tablet_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(rowset_metadata_mem_tracker, rowset_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(segment_metadata_mem_tracker, segment_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(column_metadata_mem_tracker, column_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(tablet_schema_mem_tracker, tablet_schema_mem_bytes)
    SET_MEM_METRIC_VALUE(column_zonemap_index_mem_tracker, column_zonemap_index_mem_bytes)
    SET_MEM_METRIC_VALUE(ordinal_index_mem_tracker, ordinal_index_mem_bytes)
    SET_MEM_METRIC_VALUE(bitmap_index_mem_tracker, bitmap_index_mem_bytes)
    SET_MEM_METRIC_VALUE(bloom_filter_index_mem_tracker, bloom_filter_index_mem_bytes)
    SET_MEM_METRIC_VALUE(segment_zonemap_mem_tracker, segment_zonemap_mem_bytes)
    SET_MEM_METRIC_VALUE(short_key_index_mem_tracker, short_key_index_mem_bytes)
    SET_MEM_METRIC_VALUE(compaction_mem_tracker, compaction_mem_bytes)
    SET_MEM_METRIC_VALUE(schema_change_mem_tracker, schema_change_mem_bytes)
    SET_MEM_METRIC_VALUE(page_cache_mem_tracker, storage_page_cache_mem_bytes)
    SET_MEM_METRIC_VALUE(jit_cache_mem_tracker, jit_cache_mem_bytes)
    SET_MEM_METRIC_VALUE(update_mem_tracker, update_mem_bytes)
    SET_MEM_METRIC_VALUE(passthrough_mem_tracker, passthrough_mem_bytes)
    SET_MEM_METRIC_VALUE(clone_mem_tracker, clone_mem_bytes)
    SET_MEM_METRIC_VALUE(consistency_mem_tracker, consistency_mem_bytes)
    SET_MEM_METRIC_VALUE(datacache_mem_tracker, datacache_mem_bytes)
#undef SET_MEM_METRIC_VALUE
}

void SystemMetrics::_install_disk_metrics(MetricRegistry* registry, const std::set<std::string>& devices) {
    for (auto& disk : devices) {
        auto* metrics = new DiskMetrics();
#define REGISTER_DISK_METRIC(name) \
    registry->register_metric("disk_" #name, MetricLabels().add("device", disk), &metrics->name)
        REGISTER_DISK_METRIC(reads_completed);
        REGISTER_DISK_METRIC(bytes_read);
        REGISTER_DISK_METRIC(read_time_ms);
        REGISTER_DISK_METRIC(writes_completed);
        REGISTER_DISK_METRIC(bytes_written);
        REGISTER_DISK_METRIC(write_time_ms);
        REGISTER_DISK_METRIC(io_time_ms);
        REGISTER_DISK_METRIC(io_time_weigthed);
        _disk_metrics.emplace(disk, metrics);
    }
}

void SystemMetrics::_update_disk_metrics() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_diskstats_path, "r");
#else
    FILE* fp = fopen("/proc/diskstats", "r");
#endif
    if (fp == nullptr) {
        PLOG(WARNING) << "open /proc/diskstats failed";
        return;
    }

    // /proc/diskstats: https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
    // 1 - major number
    // 2 - minor mumber
    // 3 - device name
    // 4 - reads completed successfully
    // 5 - reads merged
    // 6 - sectors read
    // 7 - time spent reading (ms)
    // 8 - writes completed
    // 9 - writes merged
    // 10 - sectors written
    // 11 - time spent writing (ms)
    // 12 - I/Os currently in progress
    // 13 - time spent doing I/Os (ms)
    // 14 - weighted time spent doing I/Os (ms)
    // I think 1024 is enougth for device name
    int major = 0;
    int minor = 0;
    char device[1024];
    int64_t values[11];
    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(_line_ptr,
                         "%d %d %1023s"
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64 " %" PRId64,
                         &major, &minor, device, &values[0], &values[1], &values[2], &values[3], &values[4], &values[5],
                         &values[6], &values[7], &values[8], &values[9], &values[10]);
        if (num < 4) {
            continue;
        }
        auto it = _disk_metrics.find(device);
        if (it == std::end(_disk_metrics)) {
            continue;
        }
        // update disk metrics
        // reads_completed: 4 reads completed successfully
        it->second->reads_completed.set_value(values[0]);
        // bytes_read: 6 sectors read * 512; 5 reads merged is ignored
        it->second->bytes_read.set_value(values[2] * 512);
        // read_time_ms: 7 time spent reading (ms)
        it->second->read_time_ms.set_value(values[3]);
        // writes_completed: 8 writes completed
        it->second->writes_completed.set_value(values[4]);
        // bytes_written: 10 sectors write * 512; 9 writes merged is ignored
        it->second->bytes_written.set_value(values[6] * 512);
        // write_time_ms: 11 time spent writing (ms)
        it->second->write_time_ms.set_value(values[7]);
        // io_time_ms: 13 time spent doing I/Os (ms)
        it->second->io_time_ms.set_value(values[9]);
        // io_time_weigthed: 14 - weighted time spent doing I/Os (ms)
        it->second->io_time_weigthed.set_value(values[10]);
    }
    if (ferror(fp) != 0) {
        PLOG(WARNING) << "getline failed";
    }
    fclose(fp);
}

void SystemMetrics::_install_net_metrics(MetricRegistry* registry, const std::vector<std::string>& interfaces) {
    for (const auto& net : interfaces) {
        auto* metrics = new NetMetrics();
#define REGISTER_NETWORK_METRIC(name) \
    registry->register_metric("network_" #name, MetricLabels().add("device", net), &metrics->name)
        REGISTER_NETWORK_METRIC(receive_bytes);
        REGISTER_NETWORK_METRIC(receive_packets);
        REGISTER_NETWORK_METRIC(send_bytes);
        REGISTER_NETWORK_METRIC(send_packets);
        _net_metrics.emplace(net, metrics);
    }
}

void SystemMetrics::_install_snmp_metrics(MetricRegistry* registry) {
    _snmp_metrics = std::make_unique<SnmpMetrics>();
#define REGISTER_SNMP_METRIC(name) \
    registry->register_metric("snmp", MetricLabels().add("name", #name), &_snmp_metrics->name)
    REGISTER_SNMP_METRIC(tcp_in_errs);
    REGISTER_SNMP_METRIC(tcp_retrans_segs);
    REGISTER_SNMP_METRIC(tcp_in_segs);
    REGISTER_SNMP_METRIC(tcp_out_segs);
}

void SystemMetrics::_update_net_metrics() {
#ifdef BE_TEST
    // to mock proc
    FILE* fp = fopen(k_ut_net_dev_path, "r");
#else
    FILE* fp = fopen("/proc/net/dev", "r");
#endif
    if (fp == nullptr) {
        PLOG(WARNING) << "open /proc/net/dev failed";
        return;
    }

    // Ignore header
    if (getline(&_line_ptr, &_line_buf_size, fp) < 0 || getline(&_line_ptr, &_line_buf_size, fp) < 0) {
        PLOG(WARNING) << "read /proc/net/dev first two line failed";
        fclose(fp);
        return;
    }
    if (_proc_net_dev_version == 0) {
        if (strstr(_line_ptr, "compressed") != nullptr) {
            _proc_net_dev_version = 3;
        } else if (strstr(_line_ptr, "bytes") != nullptr) {
            _proc_net_dev_version = 2;
        } else {
            _proc_net_dev_version = 1;
        }
    }

    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        char* ptr = strrchr(_line_ptr, ':');
        if (ptr == nullptr) {
            continue;
        }
        char* start = _line_ptr;
        while (isspace(*start)) {
            start++;
        }
        std::string interface(start, ptr - start);
        auto it = _net_metrics.find(interface);
        if (it == std::end(_net_metrics)) {
            continue;
        }
        ptr++;
        int64_t receive_bytes = 0;
        int64_t receive_packets = 0;
        int64_t send_bytes = 0;
        int64_t send_packets = 0;
        switch (_proc_net_dev_version) {
        case 3:
            // receive: bytes packets errs drop fifo frame compressed multicast
            // send:    bytes packets errs drop fifo colls carrier compressed
            sscanf(ptr,
                   " %" PRId64 " %" PRId64
                   " %*d %*d %*d %*d %*d %*d"
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d %*d %*d",
                   &receive_bytes, &receive_packets, &send_bytes, &send_packets);
            break;
        case 2:
            // receive: bytes packets errs drop fifo frame
            // send:    bytes packets errs drop fifo colls carrier
            sscanf(ptr,
                   " %" PRId64 " %" PRId64
                   " %*d %*d %*d %*d"
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d %*d",
                   &receive_bytes, &receive_packets, &send_bytes, &send_packets);
            break;
        case 1:
            // receive: packets errs drop fifo frame
            // send: packets errs drop fifo colls carrier
            sscanf(ptr,
                   " %" PRId64
                   " %*d %*d %*d %*d"
                   " %" PRId64 " %*d %*d %*d %*d %*d",
                   &receive_packets, &send_packets);
            break;
        default:
            break;
        }
        it->second->receive_bytes.set_value(receive_bytes);
        it->second->receive_packets.set_value(receive_packets);
        it->second->send_bytes.set_value(send_bytes);
        it->second->send_packets.set_value(send_packets);
    }
    if (ferror(fp) != 0) {
        PLOG(WARNING) << "getline failed";
    }
    fclose(fp);
}

void SystemMetrics::_update_snmp_metrics() {
#ifdef BE_TEST
    // to mock proc
    FILE* fp = fopen(k_ut_net_snmp_path, "r");
#else
    FILE* fp = fopen("/proc/net/snmp", "r");
#endif
    if (fp == nullptr) {
        PLOG(WARNING) << "open /proc/net/snmp failed";
        return;
    }

    // We only care about Tcp lines, so skip other lines in front of Tcp line
    int res;
    while ((res = getline(&_line_ptr, &_line_buf_size, fp)) > 0) {
        if (strstr(_line_ptr, "Tcp") != nullptr) {
            break;
        }
    }
    if (res <= 0) {
        PLOG(WARNING) << "failed to skip lines of /proc/net/snmp";
        fclose(fp);
        return;
    }

    // parse the Tcp header
    // Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors
    std::vector<std::string> headers = strings::Split(_line_ptr, " ");
    std::unordered_map<std::string, int32_t> header_map;
    int32_t pos = 0;
    for (auto& h : headers) {
        header_map.emplace(h, pos++);
    }

    // read the metrics of TCP
    if (getline(&_line_ptr, &_line_buf_size, fp) < 0) {
        PLOG(WARNING) << "failed to skip Tcp header line of /proc/net/snmp";
        fclose(fp);
        return;
    }

    // metric line looks like:
    // Tcp: 1 200 120000 -1 47849374 38601877 3353843 2320314 276 1033354613 1166025166 825439 12694 23238924 0
    std::vector<std::string> metrics = strings::Split(_line_ptr, " ");
    if (metrics.size() != headers.size()) {
        LOG(WARNING) << "invalid tcp metrics line: " << _line_ptr;
        fclose(fp);
        return;
    }
    int64_t retrans_segs = atoi64(metrics[header_map["RetransSegs"]]);
    int64_t in_errs = atoi64(metrics[header_map["InErrs"]]);
    int64_t in_segs = atoi64(metrics[header_map["InSegs"]]);
    int64_t out_segs = atoi64(metrics[header_map["OutSegs"]]);
    _snmp_metrics->tcp_retrans_segs.set_value(retrans_segs);
    _snmp_metrics->tcp_in_errs.set_value(in_errs);
    _snmp_metrics->tcp_in_segs.set_value(in_segs);
    _snmp_metrics->tcp_out_segs.set_value(out_segs);

    if (ferror(fp) != 0) {
        PLOG(WARNING) << "getline failed";
    }
    fclose(fp);
}

void SystemMetrics::_update_query_cache_metrics() {
    auto* cache_mgr = ExecEnv::GetInstance()->cache_mgr();
    if (UNLIKELY(cache_mgr == nullptr)) {
        return;
    }
    auto capacity = cache_mgr->capacity();
    auto usage = cache_mgr->memory_usage();
    auto lookup_count = cache_mgr->lookup_count();
    auto hit_count = cache_mgr->hit_count();
    auto usage_ratio = (capacity == 0L) ? 0.0 : double(usage) / double(capacity);
    auto hit_ratio = (lookup_count == 0L) ? 0.0 : double(hit_count) / double(lookup_count);
    _query_cache_metrics->query_cache_capacity.set_value(capacity);
    _query_cache_metrics->query_cache_usage.set_value(usage);
    _query_cache_metrics->query_cache_usage_ratio.set_value(usage_ratio);
    _query_cache_metrics->query_cache_lookup_count.set_value(lookup_count);
    _query_cache_metrics->query_cache_hit_count.set_value(hit_count);
    _query_cache_metrics->query_cache_hit_ratio.set_value(hit_ratio);
}

void SystemMetrics::_install_vector_index_cache_metrics(MetricRegistry* registry) {
    _vector_index_cache_metrics = std::make_unique<VectorIndexCacheMetrics>();
    registry->register_metric("vector_index_cache_capacity", &_vector_index_cache_metrics->vector_index_cache_capacity);
    registry->register_metric("vector_index_cache_usage", &_vector_index_cache_metrics->vector_index_cache_usage);
    registry->register_metric("vector_index_cache_usage_ratio",
                              &_vector_index_cache_metrics->vector_index_cache_usage_ratio);
    registry->register_metric("vector_index_cache_lookup_count",
                              &_vector_index_cache_metrics->vector_index_cache_lookup_count);
    registry->register_metric("vector_index_cache_hit_count",
                              &_vector_index_cache_metrics->vector_index_cache_hit_count);
    registry->register_metric("vector_index_cache_hit_ratio",
                              &_vector_index_cache_metrics->vector_index_cache_hit_ratio);
    registry->register_metric("vector_index_cache_dynamic_lookup_count",
                              &_vector_index_cache_metrics->vector_index_cache_dynamic_lookup_count);
    registry->register_metric("vector_index_cache_dynamic_hit_count",
                              &_vector_index_cache_metrics->vector_index_cache_dynamic_hit_count);
    registry->register_metric("vector_index_cache_dynamic_hit_ratio",
                              &_vector_index_cache_metrics->vector_index_cache_dynamic_hit_ratio);
}

void SystemMetrics::_update_vector_index_cache_metrics() {
#ifdef WITH_TENANN
    auto* index_cache = tenann::IndexCache::GetGlobalInstance();
    if (UNLIKELY(index_cache == nullptr)) {
        return;
    }
    auto capacity = index_cache->capacity();
    auto usage = index_cache->memory_usage();
    auto lookup_count = index_cache->lookup_count();
    auto hit_count = index_cache->hit_count();
#else
    auto capacity = 0;
    auto usage = 0;
    auto lookup_count = 0;
    auto hit_count = 0;
#endif
    auto usage_ratio = (capacity == 0L) ? 0.0 : double(usage) / double(capacity);
    auto hit_ratio = (lookup_count == 0L) ? 0.0 : double(hit_count) / double(lookup_count);
    auto dynamic_lookup_count = lookup_count - _vector_index_cache_metrics->_previous_lookup_count;
    auto dynamic_hit_count = hit_count - _vector_index_cache_metrics->_previous_hit_count;
    auto dynamic_hit_ratio =
            (dynamic_lookup_count == 0) ? 0.0 : double(dynamic_hit_count) / double(dynamic_lookup_count);
    _vector_index_cache_metrics->vector_index_cache_capacity.set_value(capacity);
    _vector_index_cache_metrics->vector_index_cache_usage.set_value(usage);
    _vector_index_cache_metrics->vector_index_cache_usage_ratio.set_value(usage_ratio);
    _vector_index_cache_metrics->vector_index_cache_lookup_count.set_value(lookup_count);
    _vector_index_cache_metrics->vector_index_cache_hit_count.set_value(hit_count);
    _vector_index_cache_metrics->vector_index_cache_hit_ratio.set_value(hit_ratio);
    _vector_index_cache_metrics->vector_index_cache_dynamic_lookup_count.set_value(dynamic_lookup_count);
    _vector_index_cache_metrics->vector_index_cache_dynamic_hit_count.set_value(dynamic_hit_count);
    _vector_index_cache_metrics->vector_index_cache_dynamic_hit_ratio.set_value(dynamic_hit_ratio);

    _vector_index_cache_metrics->_previous_lookup_count = lookup_count;
    _vector_index_cache_metrics->_previous_hit_count = hit_count;
}

void SystemMetrics::_install_fd_metrics(MetricRegistry* registry) {
    _fd_metrics = std::make_unique<FileDescriptorMetrics>();
    registry->register_metric("fd_num_limit", &_fd_metrics->fd_num_limit);
    registry->register_metric("fd_num_used", &_fd_metrics->fd_num_used);
}

void SystemMetrics::_install_query_cache_metrics(starrocks::MetricRegistry* registry) {
    _query_cache_metrics = std::make_unique<QueryCacheMetrics>();
    registry->register_metric("query_cache_capacity", &_query_cache_metrics->query_cache_capacity);
    registry->register_metric("query_cache_usage", &_query_cache_metrics->query_cache_usage);
    registry->register_metric("query_cache_usage_ratio", &_query_cache_metrics->query_cache_usage_ratio);
    registry->register_metric("query_cache_lookup_count", &_query_cache_metrics->query_cache_lookup_count);
    registry->register_metric("query_cache_hit_count", &_query_cache_metrics->query_cache_hit_count);
    registry->register_metric("query_cache_hit_ratio", &_query_cache_metrics->query_cache_hit_ratio);
}

void SystemMetrics::_install_runtime_filter_metrics(starrocks::MetricRegistry* registry) {
    for (int i = 0; i < EventType::MAX_COUNT; i++) {
        auto* metrics = new RuntimeFilterMetrics();
        const auto& type = EventTypeToString((EventType)i);
#define REGISTER_RUNTIME_FILTER_METRIC(name) \
    registry->register_metric(#name, MetricLabels().add("type", type), &metrics->name)
        REGISTER_RUNTIME_FILTER_METRIC(runtime_filter_events_in_queue);
        REGISTER_RUNTIME_FILTER_METRIC(runtime_filter_bytes_in_queue);
        _runtime_filter_metrics.emplace(type, metrics);
    }
}

void SystemMetrics::_update_runtime_filter_metrics() {
    auto* runtime_filter_worker = ExecEnv::GetInstance()->runtime_filter_worker();
    if (UNLIKELY(runtime_filter_worker == nullptr)) {
        return;
    }
    const auto* metrics = runtime_filter_worker->metrics();
    for (int i = 0; i < EventType::MAX_COUNT; i++) {
        const auto& event_name = EventTypeToString((EventType)i);
        auto iter = _runtime_filter_metrics.find(event_name);
        if (iter == _runtime_filter_metrics.end()) {
            continue;
        }
        iter->second->runtime_filter_events_in_queue.set_value(metrics->event_nums[i]);
        iter->second->runtime_filter_bytes_in_queue.set_value(metrics->runtime_filter_bytes[i]);
    }
}

void SystemMetrics::_update_fd_metrics() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_fd_path, "r");
#else
    FILE* fp = fopen("/proc/sys/fs/file-nr", "r");
#endif
    if (fp == nullptr) {
        PLOG(WARNING) << "open /proc/sys/fs/file-nr failed";
        return;
    }

    // /proc/sys/fs/file-nr: https://www.kernel.org/doc/Documentation/sysctl/fs.txt
    // 1 - the number of allocated file handles
    // 2 - the number of allocated but unused file handles
    // 3 - the maximum number of file handles

    int64_t values[3];
    if (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(_line_ptr, "%" PRId64 " %" PRId64 " %" PRId64, &values[0], &values[1], &values[2]);
        if (num == 3) {
            _fd_metrics->fd_num_limit.set_value(values[2]);
            _fd_metrics->fd_num_used.set_value(values[0] - values[1]);
        }
    }

    if (ferror(fp) != 0) {
        PLOG(WARNING) << "getline failed";
    }
    fclose(fp);
}

int64_t SystemMetrics::get_max_io_util(const std::map<std::string, int64_t>& lst_value, int64_t interval_sec) {
    int64_t max = 0;
    for (auto& it : _disk_metrics) {
        int64_t cur = it.second->io_time_ms.value();
        const auto find = lst_value.find(it.first);
        if (find == lst_value.end()) {
            continue;
        }
        int64_t incr = cur - find->second;
        if (incr > max) max = incr;
    }
    return max / interval_sec / 10;
}

void SystemMetrics::get_disks_io_time(std::map<std::string, int64_t>* map) {
    map->clear();
    for (auto& it : _disk_metrics) {
        map->emplace(it.first, it.second->io_time_ms.value());
    }
}

void SystemMetrics::get_network_traffic(std::map<std::string, int64_t>* send_map,
                                        std::map<std::string, int64_t>* rcv_map) {
    send_map->clear();
    rcv_map->clear();
    for (auto& it : _net_metrics) {
        if (it.first == "lo") {
            continue;
        }
        send_map->emplace(it.first, it.second->send_bytes.value());
        rcv_map->emplace(it.first, it.second->receive_bytes.value());
    }
}

void SystemMetrics::get_max_net_traffic(const std::map<std::string, int64_t>& lst_send_map,
                                        const std::map<std::string, int64_t>& lst_rcv_map, int64_t interval_sec,
                                        int64_t* send_rate, int64_t* rcv_rate) {
    int64_t max_send = 0;
    int64_t max_rcv = 0;
    for (auto& it : _net_metrics) {
        int64_t cur_send = it.second->send_bytes.value();
        int64_t cur_rcv = it.second->receive_bytes.value();

        const auto find_send = lst_send_map.find(it.first);
        if (find_send != lst_send_map.end()) {
            int64_t incr = cur_send - find_send->second;
            if (incr > max_send) max_send = incr;
        }
        const auto find_rcv = lst_rcv_map.find(it.first);
        if (find_rcv != lst_rcv_map.end()) {
            int64_t incr = cur_rcv - find_rcv->second;
            if (incr > max_rcv) max_rcv = incr;
        }
    }

    *send_rate = max_send / interval_sec;
    *rcv_rate = max_rcv / interval_sec;
}

void SystemMetrics::_install_io_metrics(MetricRegistry* registry) {
    for (uint32_t i = 0; i < IOProfiler::TAG::TAG_END; i++) {
        std::string tag_name = IOProfiler::tag_to_string(i);
        auto* metrics = new IOMetrics();
#define REGISTER_IO_METRIC(name) \
    registry->register_metric("io_" #name, MetricLabels().add("tag", tag_name), &metrics->name);
        REGISTER_IO_METRIC(read_ops);
        REGISTER_IO_METRIC(read_bytes);
        REGISTER_IO_METRIC(write_ops);
        REGISTER_IO_METRIC(write_bytes);

        _io_metrics.emplace_back(metrics);
    }
}
} // namespace starrocks
