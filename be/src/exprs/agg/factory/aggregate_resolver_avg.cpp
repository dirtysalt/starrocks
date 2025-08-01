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

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/array_union_agg.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"

namespace starrocks {

struct AvgDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> && !lt_is_string<lt>) {
            auto func = AggregateFactory::MakeAvgAggregateFunction<lt>();
            using AvgState = AvgAggregateState<RunTimeCppType<ImmediateAvgResultLT<lt>>>;
            resolver->add_aggregate_mapping<lt, AvgResultLT<lt>, AvgState>("avg", true, func);
        }
    }
};

struct ArrayAggDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> || lt_is_json<lt>) {
            auto func = std::make_shared<ArrayAggAggregateFunction<lt, false>>();
            using AggState = ArrayAggAggregateState<lt, false>;
            resolver->add_aggregate_mapping<lt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>("array_agg", false,
                                                                                                   func);
        }
    }
};

struct ArrayUniqueAggDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<pt>) {
            using CppType = RunTimeCppType<pt>;
            if constexpr (lt_is_largeint<pt>) {
                using MyHashSet = phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>;
                auto func = std::make_shared<ArrayUnionAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayUnionAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "array_unique_agg", false, func);
            } else if constexpr (lt_is_fixedlength<pt>) {
                using MyHashSet = phmap::flat_hash_set<CppType, StdHash<CppType>>;
                auto func = std::make_shared<ArrayUnionAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayUnionAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "array_unique_agg", false, func);
            } else if constexpr (lt_is_string<pt>) {
                using MyHashSet = SliceHashSet;
                auto func = std::make_shared<ArrayUnionAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayUnionAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "array_unique_agg", false, func);
            } else {
                throw std::runtime_error("array_unique_agg does not support " + type_to_string(pt));
            }
        }
    }
};

struct ArrayAggDistinctDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<pt>) {
            using CppType = RunTimeCppType<pt>;
            if constexpr (lt_is_largeint<pt>) {
                using MyHashSet = phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>;
                auto func = std::make_shared<ArrayAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "array_agg_distinct", false, func);
            } else if constexpr (lt_is_fixedlength<pt>) {
                using MyHashSet = phmap::flat_hash_set<CppType, StdHash<CppType>>;
                auto func = std::make_shared<ArrayAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "array_agg_distinct", false, func);
            } else if constexpr (lt_is_string<pt>) {
                using MyHashSet = SliceHashSet;
                auto func = std::make_shared<ArrayAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "array_agg_distinct", false, func);
            } else {
                throw std::runtime_error("array_agg_distinct does not support " + type_to_string(pt));
            }
        }
    }
};

struct MapAggDispatcher {
    template <LogicalType kt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<kt>) {
            using KeyCppType = RunTimeCppType<kt>;
            if constexpr (lt_is_largeint<kt>) {
                using MyHashMap = phmap::flat_hash_map<KeyCppType, size_t, Hash128WithSeed<PhmapSeed1>>;
                auto func = std::make_shared<MapAggAggregateFunction<kt, MyHashMap>>();
                resolver->add_aggregate_mapping_notnull<kt, TYPE_MAP, AggregateFunctionPtr>("map_agg", false, func);
            } else if constexpr (lt_is_fixedlength<kt>) {
                using MyHashMap = phmap::flat_hash_map<KeyCppType, size_t, StdHash<KeyCppType>>;
                auto func = std::make_shared<MapAggAggregateFunction<kt, MyHashMap>>();
                resolver->add_aggregate_mapping_notnull<kt, TYPE_MAP, AggregateFunctionPtr>("map_agg", false, func);
            } else if constexpr (lt_is_string<kt>) {
                using MyHashMap =
                        phmap::flat_hash_map<SliceWithHash, size_t, HashOnSliceWithHash, EqualOnSliceWithHash>;
                auto func = std::make_shared<MapAggAggregateFunction<kt, MyHashMap>>();
                resolver->add_aggregate_mapping_notnull<kt, TYPE_MAP, AggregateFunctionPtr>("map_agg", false, func);
            } else {
                throw std::runtime_error("map_agg does not support key type " + type_to_string(kt));
            }
        }
    }
};

void AggregateFuncResolver::register_avg() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, AvgDispatcher(), this);
        if (type != TYPE_DECIMAL256) {
            type_dispatch_all(type, ArrayAggDispatcher(), this);
            type_dispatch_all(type, ArrayAggDistinctDispatcher(), this);
            type_dispatch_all(type, ArrayUniqueAggDispatcher(), this);
            type_dispatch_all(type, MapAggDispatcher(), this);
        }
    }
    type_dispatch_all(TYPE_JSON, ArrayAggDispatcher(), this);
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL256, TYPE_DECIMAL256, true>("decimal_avg");
}

} // namespace starrocks
