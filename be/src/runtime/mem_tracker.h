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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mem_tracker.h

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

#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"

namespace starrocks {

class MemTracker;
class RuntimeState;

/// A MemTracker tracks memory consumption; it contains an optional limit
/// and can be arranged into a tree structure such that the consumption tracked
/// by a MemTracker is also tracked by its ancestors.
///
/// We use a five-level hierarchy of mem trackers: process, pool, query, fragment
/// instance. Specific parts of the fragment (exec nodes, sinks, etc) will add a
/// fifth level when they are initialized. This function also initializes a user
/// function mem tracker (in the fifth level).
///
/// By default, memory consumption is tracked via calls to Consume()/Release(), either to
/// the tracker itself or to one of its descendents. Alternatively, a consumption metric
/// can specified, and then the metric's value is used as the consumption rather than the
/// tally maintained by Consume() and Release(). A tcmalloc metric is used to track
/// process memory consumption, since the process memory usage may be higher than the
/// computed total memory (tcmalloc does not release deallocated memory immediately).
//
/// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
/// reached. If LimitExceeded() is called and the limit is exceeded, it will first call
/// the GcFunctions to try to free memory and recheck the limit. For example, the process
/// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
/// this will be called before the process limit is reported as exceeded. GcFunctions are
/// called in the order they are added, so expensive functions should be added last.
/// GcFunctions are called with a global lock held, so should be non-blocking and not
/// call back into MemTrackers, except to release memory.
//
/// This class is thread-safe.

enum class MemTrackerType {
    NO_SET,
    PROCESS,
    QUERY,
    QUERY_POOL,
    LOAD,
    CONSISTENCY,
    COMPACTION_TASK,
    COMPACTION,
    SCHEMA_CHANGE_TASK,
    SCHEMA_CHANGE,
    RESOURCE_GROUP,
    RESOURCE_GROUP_BIG_QUERY,
    JEMALLOC,
    PASSTHROUGH,
    CONNECTOR_SCAN,
    METADATA,
    TABLET_METADATA,
    ROWSET_METADATA,
    SEGMENT_METADATA,
    COLUMN_METADATA,
    TABLET_SCHEMA,
    SEGMENT_ZONEMAP,
    SHORT_KEY_INDEX,
    COLUMN_ZONEMAP_INDEX,
    ORDINAL_INDEX,
    BITMAP_INDEX,
    BLOOM_FILTER_INDEX,
    PAGE_CACHE,
    JIT_CACHE,
    UPDATE,
    CLONE,
    DATACACHE,
    POCO_CONNECTION_POOL,
    REPLICATION,
    ROWSET_UPDATE_STATE,
    INDEX_CACHE,
    DEL_VEC_CACHE,
    COMPACTION_STATE
};

class MemTracker {
public:
    // I want to get a snapshot of the mem_tracker, but don't want to copy all the field of MemTracker.
    // SimpleItem contains the most important field of MemTracker.
    // Current this is only used for list_mem_usage
    // TODO: use a better name?
    struct SimpleItem {
        std::string label;
        size_t level = 0;
        int64_t limit = 0;
        int64_t cur_consumption = 0;
        int64_t peak_consumption = 0;
        std::vector<SimpleItem*> childs;
        SimpleItem* parent = nullptr;

        std::string debug_string() const {
            std::stringstream ss;
            ss << "{";
            ss << R"("label:")" << label << "\",";
            ss << R"("level:")" << level << "\",";
            ss << R"("limit:")" << limit << "\",";
            ss << R"("cur_mem_usage:")" << cur_consumption << "\",";
            ss << R"("peak_mem_usage:")" << peak_consumption << "\",";
            ss << R"("child":[)";
            for (size_t i = 0; i < childs.size(); i++) {
                if (i != 0) {
                    ss << ",";
                }
                ss << childs[i]->debug_string();
            }
            ss << "]}";
            return ss.str();
        }
    };

    static void init_type_label_map();

    static std::vector<std::pair<MemTrackerType, std::string>>& mem_types();
    static std::string type_to_label(MemTrackerType type);
    static MemTrackerType label_to_type(const std::string& label);

    /// 'byte_limit' < 0 means no limit
    /// 'label' is the label used in the usage string (LogUsage())
    /// If 'auto_unregister' is true, never call unregister_from_parent().
    /// If 'log_usage_if_zero' is false, this tracker (and its children) will not be included
    /// in LogUsage() output if consumption is 0.
    explicit MemTracker(int64_t byte_limit = -1, std::string label = std::string(), MemTracker* parent = nullptr);

    explicit MemTracker(MemTrackerType type, int64_t byte_limit = -1, std::string label = std::string(),
                        MemTracker* parent = nullptr);

    /// C'tor for tracker for which consumption counter is created as part of a profile.
    /// The counter is created with name COUNTER_NAME.
    explicit MemTracker(RuntimeProfile* profile, std::tuple<bool, bool, bool> attaching_info = {true, true, true},
                        const std::string& counter_name_prefix = std::string(), int64_t byte_limit = -1,
                        std::string label = std::string(), MemTracker* parent = nullptr);

    void set_level(int64_t level) { _level = level; }
    int64_t get_level() const { return _level; }

    ~MemTracker();

    // Removes this tracker from _parent->_child_trackers.
    void unregister_from_parent() {
        DCHECK(_parent != nullptr);
        std::lock_guard<std::mutex> l(_parent->_child_trackers_lock);
        _parent->_child_trackers.erase(_child_tracker_it);
        _child_tracker_it = _parent->_child_trackers.end();
    }

    // used for single mem_tracker
    void set(int64_t bytes) { _consumption->set(bytes); }

    void update_allocation(int64_t bytes) {
        if (bytes <= 0) return;
        for (auto* tracker : _all_trackers) {
            tracker->_allocation->update(bytes);
        }
    }

    void update_deallocation(int64_t bytes) {
        if (bytes <= 0) return;
        for (auto* tracker : _all_trackers) {
            tracker->_deallocation->update(bytes);
        }
    }

    void consume(int64_t bytes) {
        if (bytes == 0) {
            return;
        }
        for (auto* tracker : _all_trackers) {
            tracker->_consumption->add(bytes);
        }
    }

    // the function can be used to transform memory stats from process mem_tracker to child mem_tracker
    void consume_without_root(int64_t bytes) {
        if (bytes == 0) {
            return;
        }
        for (size_t i = 0; i < _all_trackers.size() - 1; i++) {
            _all_trackers[i]->_consumption->add(bytes);
        }
    }

    void release_without_root() { return release_without_root(consumption()); }

    SimpleItem* get_snapshot(ObjectPool* pool, size_t upper_level) const {
        return _get_snapshot_internal(pool, nullptr, upper_level);
    }

    /// Increases consumption of this tracker and its ancestors by 'bytes' only if
    /// they can all consume 'bytes'. If this brings any of them over, none of them
    /// are updated.
    /// Returns nullptr if the try succeeded, otherwise return the tracker that failed.
    WARN_UNUSED_RESULT
    MemTracker* try_consume(int64_t bytes) {
        if (UNLIKELY(bytes <= 0)) return nullptr;
        int64_t i;
        // Walk the tracker tree top-down.
        for (i = _all_trackers.size() - 1; i >= 0; --i) {
            MemTracker* tracker = _all_trackers[i];
            const int64_t limit = tracker->limit();
            if (limit < 0) {
                tracker->_consumption->add(bytes); // No limit at this tracker.
            } else {
                if (LIKELY(tracker->_consumption->try_add(bytes, limit))) {
                    continue;
                } else {
                    // Failed for this mem tracker. Roll back the ones that succeeded.
                    for (int64_t j = _all_trackers.size() - 1; j > i; --j) {
                        _all_trackers[j]->_consumption->add(-bytes);
                    }
                    return tracker;
                }
            }
        }
        // Everyone succeeded, return.
        DCHECK_EQ(i, -1);
        return nullptr;
    }

    WARN_UNUSED_RESULT
    MemTracker* try_consume_with_limited(int64_t bytes) {
        if (UNLIKELY(bytes <= 0)) return nullptr;
        int64_t i;
        // Walk the tracker tree top-down.
        for (i = _all_trackers.size() - 1; i >= 0; --i) {
            MemTracker* tracker = _all_trackers[i];
            int64_t limit = tracker->reserve_limit();
            if (limit < 0) {
                limit = tracker->limit();
            }
            if (limit < 0) {
                DCHECK_EQ(limit, -1);
                tracker->_consumption->add(bytes); // No limit at this tracker.
            } else {
                if (LIKELY(tracker->_consumption->try_add(bytes, limit))) {
                    continue;
                } else {
                    // Failed for this mem tracker. Roll back the ones that succeeded.
                    for (int64_t j = _all_trackers.size() - 1; j > i; --j) {
                        _all_trackers[j]->_consumption->add(-bytes);
                    }
                    return tracker;
                }
            }
        }
        // Everyone succeeded, return.
        DCHECK_EQ(i, -1);
        return nullptr;
    }

    /// Decreases consumption of this tracker and its ancestors by 'bytes'.
    void release(int64_t bytes) {
        if (bytes == 0) {
            return;
        }
        for (auto* tracker : _all_trackers) {
            tracker->_consumption->add(-bytes);
        }
    }

    void release_without_root(int64_t bytes) {
        if (bytes == 0 || _all_trackers.empty()) {
            return;
        }

        for (size_t i = 0; i < _all_trackers.size() - 1; i++) {
            _all_trackers[i]->_consumption->add(-bytes);
        }
    }

    // Returns true if a valid limit of this tracker or one of its ancestors is exceeded.
    bool any_limit_exceeded() {
        for (auto& _limit_tracker : _limit_trackers) {
            if (_limit_tracker->limit_exceeded()) {
                return true;
            }
        }
        return false;
    }

    // Return limit exceeded tracker or null
    MemTracker* find_limit_exceeded_tracker() const {
        for (auto& _limit_tracker : _limit_trackers) {
            if (_limit_tracker->limit_exceeded()) {
                return _limit_tracker;
            }
        }
        return nullptr;
    }

    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }

    bool limit_exceeded_by_ratio(int64_t ratio) const { return _limit >= 0 && (_limit * ratio / 100) < consumption(); }

    bool limit_exceeded_precheck(int64_t consume) const { return _limit >= 0 && _limit < consumption() + consume; }

    bool any_limit_exceeded_precheck(int64_t consume) const {
        for (auto& _limit_tracker : _limit_trackers) {
            if (_limit_tracker->limit_exceeded_precheck(consume)) {
                return true;
            }
        }
        return false;
    }

    void set_limit(int64_t limit) { _limit = limit; }

    int64_t limit() const { return _limit; }

    bool has_limit() const { return _limit >= 0; }

    void set_reserve_limit(int64_t reserve_limit) { _reserve_limit = reserve_limit; }

    int64_t reserve_limit() const { return _reserve_limit; }

    const std::string& label() const { return _label; }

    /// Returns the lowest limit for this tracker and its ancestors. Returns
    /// -1 if there is no limit.
    int64_t lowest_limit() const {
        if (_limit_trackers.empty()) return -1;
        int64_t v = std::numeric_limits<int64_t>::max();
        for (auto _limit_tracker : _limit_trackers) {
            DCHECK(_limit_tracker->has_limit());
            v = std::min(v, _limit_tracker->limit());
        }
        return v;
    }

    int64_t consumption() const { return _consumption->current_value(); }

    int64_t peak_consumption() const { return _consumption->value(); }
    int64_t allocation() const { return _allocation->value(); }
    int64_t deallocation() const { return _deallocation->value(); }

    MemTracker* parent() const { return _parent; }

    Status check_mem_limit(const std::string& msg) const;

    std::string err_msg(const std::string& msg, RuntimeState* state = nullptr) const;

    static const std::string PEAK_MEMORY_USAGE;
    static const std::string ALLOCATED_MEMORY_USAGE;
    static const std::string DEALLOCATED_MEMORY_USAGE;

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "reserve_limit: " << _reserve_limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "allocation: " << _allocation->value() << "; "
            << "deallocation: " << _deallocation->value() << "; "
            << "label: " << _label << "; "
            << "all tracker size: " << _all_trackers.size() << "; "
            << "limit trackers size: " << _limit_trackers.size() << "; "
            << "parent is null: " << ((_parent == nullptr) ? "true" : "false") << "; ";
        return msg.str();
    }

    // no any memory allocate
    size_t debug_string(char* dst, size_t max_length) {
        return snprintf(dst, max_length, "tracker:%s consumption: %ld\n", _label.c_str(),
                        _consumption->current_value());
    }

    MemTrackerType type() const { return _type; }

    std::list<MemTracker*> _child_trackers;

    std::list<MemTracker*> getChild() { return _child_trackers; }

private:
    // Walks the MemTracker hierarchy and populates _all_trackers and _limit_trackers
    void Init();

    // Adds tracker to _child_trackers
    void add_child_tracker(MemTracker* tracker) {
        std::lock_guard<std::mutex> l(_child_trackers_lock);
        tracker->_child_tracker_it = _child_trackers.insert(_child_trackers.end(), tracker);
    }

    SimpleItem* _get_snapshot_internal(ObjectPool* pool, SimpleItem* parent, size_t upper_level) const;

    MemTrackerType _type{MemTrackerType::NO_SET};

    int64_t _level = 1;
    int64_t _limit;              // in bytes
    int64_t _reserve_limit = -1; // only used in spillable query

    std::string _label;
    MemTracker* _parent;

    /// in bytes; not owned
    RuntimeProfile::HighWaterMarkCounter* _consumption;

    /// holds _consumption counter if not tied to a profile
    RuntimeProfile::HighWaterMarkCounter _local_consumption_counter;

    /// in bytes; not owned. Only record allocation but ignore deallocation
    /// And for sake of performance, it can only be updated through `update_allocation`
    RuntimeProfile::Counter* _allocation;

    /// holds _allocation counter if not tied to a profile
    RuntimeProfile::Counter _local_allocation_counter;

    /// in bytes; not owned. Only record deallocation but ignore allocation
    /// And for sake of performance, it can only be updated through `update_deallocation`
    RuntimeProfile::Counter* _deallocation;

    /// holds _deallocation counter if not tied to a profile
    RuntimeProfile::Counter _local_deallocation_counter;

    std::vector<MemTracker*> _all_trackers;   // this tracker plus all of its ancestors
    std::vector<MemTracker*> _limit_trackers; // _all_trackers with valid limits

    // All the child trackers of this tracker. Used for error reporting only.
    // i.e., Updating a parent tracker does not update the children.
    mutable std::mutex _child_trackers_lock;
    // Iterator into _parent->_child_trackers for this object. Stored to have O(1)
    // remove.
    std::list<MemTracker*>::iterator _child_tracker_it;
};

#define MEM_TRACKER_SAFE_CONSUME(mem_tracker, mem_bytes) \
    if (LIKELY((mem_tracker) != nullptr)) {              \
        (mem_tracker)->consume(mem_bytes);               \
    }

#define MEM_TRACKER_SAFE_RELEASE(mem_tracker, mem_bytes) \
    if (LIKELY((mem_tracker) != nullptr)) {              \
        (mem_tracker)->release(mem_bytes);               \
    }

} // namespace starrocks
