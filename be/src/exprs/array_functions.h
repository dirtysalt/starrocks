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

#include "exprs/array_functions.tpp"
#include "exprs/function_context.h"

namespace starrocks {

class ArrayFunctions {
public:
    DEFINE_VECTORIZED_FN(array_length);

    DEFINE_VECTORIZED_FN(array_append);

    DEFINE_VECTORIZED_FN(array_remove);

    DEFINE_VECTORIZED_FN(array_contains_generic);

    template <LogicalType LT>
    static StatusOr<ColumnPtr> array_contains_specific(FunctionContext* context, const Columns& columns) {
        return ArrayContains<LT, false, UInt8Column>::process(context, columns);
    }
    template <LogicalType LT>
    static Status array_contains_specific_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return ArrayContains<LT, false, UInt8Column>::prepare(context, scope);
    }
    template <LogicalType LT>
    static Status array_contains_specific_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return ArrayContains<LT, false, UInt8Column>::close(context, scope);
    }

    DEFINE_VECTORIZED_FN(array_position_generic);

    template <LogicalType LT>
    static StatusOr<ColumnPtr> array_position_specific(FunctionContext* context, const Columns& columns) {
        return ArrayContains<LT, true, Int32Column>::process(context, columns);
    }
    template <LogicalType LT>
    static Status array_position_specific_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return ArrayContains<LT, true, Int32Column>::prepare(context, scope);
    }
    template <LogicalType LT>
    static Status array_position_specific_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return ArrayContains<LT, true, Int32Column>::close(context, scope);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_distinct(FunctionContext* context, const Columns& columns) {
        return ArrayDistinct<type>::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_difference(FunctionContext* context, const Columns& columns) {
        return ArrayDifference<type>::process(context, columns);
    }

    DEFINE_VECTORIZED_FN(array_slice);

    DEFINE_VECTORIZED_FN(repeat);

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_overlap(FunctionContext* context, const Columns& columns) {
        return ArrayOverlap<type>::process(context, columns);
    }

    template <LogicalType type>
    static Status array_overlap_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return ArrayOverlap<type>::prepare(context, scope);
    }

    template <LogicalType type>
    static Status array_overlap_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return ArrayOverlap<type>::close(context, scope);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_intersect(FunctionContext* context, const Columns& columns) {
        return ArrayIntersect<type>::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_sort(FunctionContext* context, const Columns& columns) {
        return ArraySort<type>::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_sortby(FunctionContext* context, const Columns& columns) {
        return ArraySortBy<type>::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_reverse(FunctionContext* context, const Columns& columns) {
        return ArrayReverse<type>::process(context, columns);
    }

    static StatusOr<ColumnPtr> array_join(FunctionContext* context, const Columns& columns) {
        return ArrayJoin::process(context, columns);
    }

    static StatusOr<ColumnPtr> array_concat_ws(FunctionContext* context, const Columns& columns) {
        DCHECK_EQ(columns.size(), 2);
        Columns new_columns(2);
        new_columns[0] = columns[1];
        new_columns[1] = columns[0];
        return ArrayJoin::process(context, new_columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_sum(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_sum<type>(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_avg(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_avg<type>(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_min(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_min<type>(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_max(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_max<type>(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_generate(FunctionContext* context, const Columns& columns) {
        return ArrayGenerate<type>::process(context, columns);
    }

    DEFINE_VECTORIZED_FN(concat);

    DEFINE_VECTORIZED_FN(array_cum_sum_bigint);
    DEFINE_VECTORIZED_FN(array_cum_sum_double);

    DEFINE_VECTORIZED_FN(array_contains_any);

    DEFINE_VECTORIZED_FN(array_contains_all);

    template <LogicalType LT>
    static StatusOr<ColumnPtr> array_contains_all_specific(FunctionContext* context, const Columns& columns) {
        return ArrayContainsAll<LT, false>::process(context, columns);
    }
    template <LogicalType LT>
    static Status array_contains_all_specific_prepare(FunctionContext* context,
                                                      FunctionContext::FunctionStateScope scope) {
        return ArrayContainsAll<LT, false>::prepare(context, scope);
    }
    template <LogicalType LT>
    static Status array_contains_all_specific_close(FunctionContext* context,
                                                    FunctionContext::FunctionStateScope scope) {
        return ArrayContainsAll<LT, false>::close(context, scope);
    }

    DEFINE_VECTORIZED_FN(array_map);
    DEFINE_VECTORIZED_FN(array_filter);
    DEFINE_VECTORIZED_FN(all_match);
    DEFINE_VECTORIZED_FN(any_match);

    DEFINE_VECTORIZED_FN(array_contains_seq);
    template <LogicalType LT>
    static StatusOr<ColumnPtr> array_contains_seq_specific(FunctionContext* context, const Columns& columns) {
        return ArrayContainsAll<LT, true>::process(context, columns);
    }
    template <LogicalType LT>
    static Status array_contains_seq_specific_prepare(FunctionContext* context,
                                                      FunctionContext::FunctionStateScope scope) {
        return ArrayContainsAll<LT, true>::prepare(context, scope);
    }
    template <LogicalType LT>
    static Status array_contains_seq_specific_close(FunctionContext* context,
                                                    FunctionContext::FunctionStateScope scope) {
        return ArrayContainsAll<LT, true>::close(context, scope);
    }

    // array function for nested type(Array/Map/Struct)
    DEFINE_VECTORIZED_FN(array_distinct_any_type);
    DEFINE_VECTORIZED_FN(array_reverse_any_types);
    DEFINE_VECTORIZED_FN(array_intersect_any_type);

    DEFINE_VECTORIZED_FN(array_sortby_multi);

    DEFINE_VECTORIZED_FN(array_flatten);
};

} // namespace starrocks
