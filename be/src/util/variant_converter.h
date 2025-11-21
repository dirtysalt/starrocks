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

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "formats/parquet/variant.h"
#include "types/logical_type.h"
#include "variant_util.h"

namespace starrocks {

#define VARIANT_CAST_NOT_SUPPORT(variant_type, logical_type)                                                         \
    Status::NotSupported(fmt::format("Cannot cast variant({}) to type: ", VariantUtil::type_to_string(variant_type), \
                                     logical_type_to_string(logical_type)))

Status cast_variant_to_bool(const Variant& variant, ColumnBuilder<TYPE_BOOLEAN>& result);

Status cast_variant_to_string(const Variant& variant, const cctz::time_zone& zone, ColumnBuilder<TYPE_VARCHAR>& result);

template <LogicalType ResultType>
Status cast_variant_to_arithmetic(const Variant& variant, ColumnBuilder<ResultType>& result) {
    switch (const VariantType type = variant.type()) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
    case VariantType::BOOLEAN: {
        auto value = variant.get_bool();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT8: {
        auto value = variant.get_int8();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT16: {
        auto value = variant.get_int16();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT32: {
        auto value = variant.get_int32();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT64: {
        auto value = variant.get_int64();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    default:
        return VARIANT_CAST_NOT_SUPPORT(type, ResultType);
    }
}

template <LogicalType ResultType, bool AllowThrowException>
static Status cast_variant_value_to(const Variant& variant, const cctz::time_zone& zone,
                                    ColumnBuilder<ResultType>& result) {
    const VariantType variant_type = variant.type();
    // Supported types: arithmetic, string, decimal, variant
    // Some casting require more information like target type within ARRAY/MAP/STRUCT which is not available here:
    // VARIANT -> ARRAY<ANY>: CastVariantToArray
    // VARIANT -> MAP<VARCHAR, ANY>: CastVariantToMap
    // VARIANT -> STRUCT<...>: CastVariantToStruct
    if constexpr (!lt_is_arithmetic<ResultType> && !lt_is_string<ResultType> && !lt_is_decimal<ResultType> && ResultType != TYPE_VARIANT) {
        if constexpr (AllowThrowException) {
            return VARIANT_CAST_NOT_SUPPORT(variant_type, ResultType);
        }

        result.append_null();
        return Status::OK();
    }

    if (variant_type == VariantType::NULL_TYPE) {
        result.append_null();
        return Status::OK();
    }

    if constexpr (ResultType == TYPE_VARIANT) {
        result.append(VariantValue::of_variant(variant));
        return Status::OK();
    }

    Status status;
    if constexpr (ResultType == TYPE_BOOLEAN) {
        status = cast_variant_to_bool(variant, result);
    } else if constexpr (lt_is_arithmetic<ResultType>) {
        status = cast_variant_to_arithmetic<ResultType>(variant, result);
    } else if constexpr (lt_is_string<ResultType>) {
        status = cast_variant_to_string(variant, zone, result);
    }

    if (!status.ok()) {
        if constexpr (AllowThrowException) {
            return Status::InternalError(
                    fmt::format("Fail to cast variant: {}", logical_type_to_string(ResultType), status.to_string()));
        } else {
            result.append_null();
        }
    }

    return Status::OK();
}

} // namespace starrocks