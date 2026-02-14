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

#include "exprs/variant_functions.h"

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/type_traits.h"
#include "column/variant_column.h"
#include "column/variant_encoder.h"
#include "exprs/variant_converter.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"
#include "variant_path_parser.h"

namespace starrocks {

StatusOr<ColumnPtr> VariantFunctions::variant_query(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_VARIANT>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_string(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_VARCHAR>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_int(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_bool(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_BOOLEAN>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_double(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DOUBLE>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_date(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DATE>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_datetime(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DATETIME>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_time(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_TIME>(context, columns);
}

Status VariantFunctions::variant_segments_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Don't parse if the path is not constant
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    const auto path_col = context->get_constant_column(1);
    const Slice variant_path = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_col);

    std::string path_string = variant_path.to_string();
    auto variant_path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!variant_path_status.ok(), variant_path_status.status());
    auto* path_state = new VariantState();
    path_state->variant_path.reset(std::move(variant_path_status.value()));
    context->set_function_state(scope, path_state);
    VLOG(10) << "Preloaded variant path: " << path_string;

    return Status::OK();
}

static StatusOr<VariantPath*> get_or_parse_variant_segments(FunctionContext* context, const Slice path_slice,
                                                            VariantPath* variant_path) {
    auto* cached = reinterpret_cast<VariantState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (cached != nullptr) {
        // If we already have parsed segments, return them
        return &cached->variant_path;
    }

    std::string path_string = path_slice.to_string();
    auto path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!path_status.ok(), path_status.status());
    variant_path->reset(std::move(path_status.value()));

    return variant_path;
}

struct TypedPathMatch {
    int path_index = -1;
    VariantPath suffix_path;
};

template <LogicalType ResultType>
static void append_variant_field_to_result(FunctionContext* context, VariantRowValue&& field,
                                           ColumnBuilder<ResultType>* result) {
    if constexpr (ResultType == TYPE_VARIANT) {
        result->append(std::move(field));
    } else {
        const RuntimeState* state = context->state();
        cctz::time_zone zone = (state == nullptr) ? cctz::local_time_zone() : context->state()->timezone_obj();
        Status casted = cast_variant_value_to<ResultType, true>(field, zone, *result);
        if (!casted.ok()) {
            result->append_null();
        }
    }
}

static bool try_match_typed_path(const VariantColumn* variant_data_column, const VariantPath* full_path,
                                 TypedPathMatch* match) {
    if (variant_data_column == nullptr || full_path == nullptr || match == nullptr ||
        !variant_data_column->is_shredded_variant()) {
        return false;
    }

    // Root typed path fast path: key == "".
    int root_path_index = variant_data_column->find_shredded_path("");
    if (root_path_index >= 0) {
        match->path_index = root_path_index;
        match->suffix_path.segments = full_path->segments;
        return true;
    }

    std::vector<std::string_view> object_keys;
    size_t object_prefix_len = 0;
    for (size_t i = 0; i < full_path->segments.size(); ++i) {
        const auto* object = std::get_if<VariantObjectExtraction>(&full_path->segments[i]);
        if (object == nullptr) {
            break;
        }
        object_keys.emplace_back(object->get_key());
        object_prefix_len++;
    }

    if (object_prefix_len == 0) {
        return false;
    }

    std::string typed_key;
    for (size_t prefix_len = object_prefix_len; prefix_len > 0; --prefix_len) {
        typed_key.clear();
        for (size_t i = 0; i < prefix_len; ++i) {
            if (i > 0) {
                typed_key.push_back('.');
            }
            typed_key.append(object_keys[i]);
        }

        int path_index = variant_data_column->find_shredded_path(typed_key);
        if (path_index < 0) {
            continue;
        }

        match->path_index = path_index;
        if (prefix_len < full_path->segments.size()) {
            match->suffix_path.segments.assign(full_path->segments.begin() + prefix_len, full_path->segments.end());
        } else {
            match->suffix_path.segments.clear();
        }
        return true;
    }

    return false;
}

static StatusOr<VariantRowValue> encode_typed_datum_to_variant_row(const Datum& datum,
                                                                   const TypeDescriptor& type_desc) {
    if (datum.is_null()) {
        return VariantRowValue::from_null();
    }

    if (type_desc.type == TYPE_VARIANT) {
        const VariantRowValue* variant = datum.get_variant();
        if (variant == nullptr) {
            return Status::InvalidArgument("Typed variant datum is null");
        }
        return *variant;
    }

    auto one_row_typed_col = ColumnHelper::create_column(type_desc, true);
    one_row_typed_col->append_datum(datum);

    ColumnBuilder<TYPE_VARIANT> variant_builder(1);
    RETURN_IF_ERROR(VariantEncoder::encode_column(one_row_typed_col, type_desc, &variant_builder, false));
    auto encoded_variant_col = variant_builder.build(false);

    ColumnViewer<TYPE_VARIANT> encoded_viewer(encoded_variant_col);
    if (encoded_viewer.size() != 1 || encoded_viewer.is_null(0) || encoded_viewer.value(0) == nullptr) {
        return Status::InvalidArgument("Failed to encode typed datum to variant row");
    }
    return *encoded_viewer.value(0);
}

template <LogicalType ResultType>
static bool try_append_typed_match_result(FunctionContext* context, const VariantColumn* variant_data_column,
                                          const TypedPathMatch& typed_match, size_t variant_row,
                                          ColumnBuilder<ResultType>* result) {
    const Column* typed_column = variant_data_column->typed_column_by_index(typed_match.path_index);
    DCHECK(typed_column != nullptr);
    size_t typed_row = typed_column->is_constant() ? 0 : variant_row;
    Datum typed_datum = typed_column->get(typed_row);
    if (typed_datum.is_null()) {
        return false;
    }

    const TypeDescriptor& typed_type_desc = variant_data_column->shredded_types()[typed_match.path_index];
    const LogicalType typed_logical_type = typed_type_desc.type;
    if constexpr (ResultType != TYPE_VARIANT) {
        if (typed_match.suffix_path.segments.empty() && typed_logical_type == ResultType) {
            result->append(typed_datum.get<RunTimeCppType<ResultType>>());
            return true;
        }
    }

    StatusOr<VariantRowValue> typed_variant_row = encode_typed_datum_to_variant_row(typed_datum, typed_type_desc);
    if (!typed_variant_row.ok()) {
        return false;
    }

    StatusOr<VariantRowValue> field = typed_variant_row;
    if (!typed_match.suffix_path.segments.empty()) {
        field = VariantPath::seek(&typed_variant_row.value(), &typed_match.suffix_path);
    }
    if (!field.ok()) {
        return false;
    }

    append_variant_field_to_result<ResultType>(context, std::move(field.value()), result);
    return true;
}

Status VariantFunctions::variant_segments_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* variant_state = reinterpret_cast<VariantState*>(context->get_function_state(scope));
        delete variant_state;
    }

    return Status::OK();
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> VariantFunctions::_do_variant_query(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns.size() != 2) {
        return Status::InvalidArgument("Variant query functions requires 2 arguments");
    }

    size_t num_rows = columns[0]->size();

    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    const auto* variant_data_column = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(columns[0].get()));
    const bool is_const_path = columns[1]->is_constant();

    ColumnBuilder<ResultType> result(num_rows);
    VariantPath stored_path;

    const VariantPath* const_variant_path = nullptr;
    bool const_path_parse_failed = false;
    TypedPathMatch const_typed_match;
    bool has_const_typed_match = false;

    if (is_const_path && !path_viewer.is_null(0)) {
        auto variant_segments_status = get_or_parse_variant_segments(context, path_viewer.value(0), &stored_path);
        if (!variant_segments_status.ok()) {
            const_path_parse_failed = true;
        } else {
            const_variant_path = variant_segments_status.value();
            if (variant_data_column->is_shredded_variant()) {
                has_const_typed_match =
                        try_match_typed_path(variant_data_column, const_variant_path, &const_typed_match);
            }
        }
    }

    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row) || path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        const VariantPath* variant_path = const_variant_path;
        TypedPathMatch row_typed_match;
        const TypedPathMatch* typed_match = nullptr;

        if (is_const_path) {
            if (const_path_parse_failed || variant_path == nullptr) {
                result.append_null();
                continue;
            }
            if (has_const_typed_match) {
                typed_match = &const_typed_match;
            }
        } else {
            auto variant_segments_status = get_or_parse_variant_segments(context, path_viewer.value(row), &stored_path);
            if (!variant_segments_status.ok()) {
                result.append_null();
                continue;
            }
            variant_path = variant_segments_status.value();
            if (variant_data_column->is_shredded_variant() &&
                try_match_typed_path(variant_data_column, variant_path, &row_typed_match)) {
                typed_match = &row_typed_match;
            }
        }

        const size_t variant_row = columns[0]->is_constant() ? 0 : row;

        if (typed_match != nullptr && try_append_typed_match_result<ResultType>(context, variant_data_column,
                                                                                *typed_match, variant_row, &result)) {
            continue;
        }

        VariantRowValue variant_buffer;
        const VariantRowValue* variant = variant_data_column->get_row_value(variant_row, &variant_buffer);
        if (variant == nullptr) {
            result.append_null();
            continue;
        }

        auto field = VariantPath::seek(variant, variant_path);
        if (!field.ok()) {
            // If seek fails (e.g., path not found), append null
            result.append_null();
            continue;
        }

        append_variant_field_to_result<ResultType>(context, std::move(field.value()), &result);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> VariantFunctions::variant_typeof(FunctionContext* context, const Columns& columns) {
    const auto& variant_column = columns[0];
    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(variant_column);
    size_t num_rows = variant_column->size();
    const auto* variant_data_column =
            down_cast<const VariantColumn*>(ColumnHelper::get_data_column(variant_column.get()));

    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        const size_t variant_row = variant_column->is_constant() ? 0 : row;
        if (variant_data_column->is_typed_only_variant()) {
            if (!variant_data_column->is_root_typed_only_variant()) {
                result.append(VariantUtil::variant_type_to_string(VariantType::OBJECT));
                continue;
            }
            const Column* typed_column = variant_data_column->typed_column_by_index(0);
            DCHECK(typed_column != nullptr);
            size_t typed_row = typed_column->is_constant() ? 0 : variant_row;
            Datum typed_datum = typed_column->get(typed_row);
            if (typed_datum.is_null()) {
                result.append_null();
                continue;
            }
            auto typed_variant_row =
                    encode_typed_datum_to_variant_row(typed_datum, variant_data_column->shredded_types()[0]);
            if (!typed_variant_row.ok()) {
                result.append_null();
                continue;
            }
            result.append(VariantUtil::variant_type_to_string(typed_variant_row.value().get_value().type()));
            continue;
        }

        VariantRowValue variant_buffer;
        const VariantRowValue* variant = variant_data_column->get_row_value(variant_row, &variant_buffer);
        if (variant == nullptr) {
            result.append_null();
            continue;
        }
        result.append(VariantUtil::variant_type_to_string(variant->get_value().type()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/VariantFunctions.inc"
