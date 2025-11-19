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

#include "variant_converter.h"

#include "variant_util.h"

namespace starrocks {

StatusOr<RunTimeCppType<TYPE_BOOLEAN>> cast_variant_to_bool(const Variant& variant,
                                                            ColumnBuilder<TYPE_BOOLEAN>& result) {
    VariantType type = variant.type();
    if (type == VariantType::NULL_TYPE) {
        result.append_null();
        return Status::OK();
    }

    if (type == VariantType::BOOLEAN) {
        auto value = variant.get_bool();
        if (!value.ok()) {
            return value;
        }

        result.append(value.value());
        return Status::OK();
    }

    if (type == VariantType::STRING) {
        auto str = variant.get_string();
        if (str.ok()) {
            const char* str_value = str.value().data();
            size_t len = str.value().size();
            StringParser::ParseResult parsed;
            auto r = StringParser::string_to_int<int32_t>(str_value, len, &parsed);
            if (parsed != StringParser::PARSE_SUCCESS) {
                const bool casted = StringParser::string_to_bool(str_value, len, &parsed);
                if (parsed != StringParser::PARSE_SUCCESS) {
                    return Status::VariantError(fmt::format("Failed to cast string '{}' to BOOLEAN", str.value()));
                }

                result.append(casted);
            } else {
                result.append(r != 0);
            }

            return Status::OK();
        }
    }

    return VARIANT_CAST_NOT_SUPPORT(TYPE_BOOLEAN);
}

StatusOr<RunTimeCppType<TYPE_VARCHAR>> cast_variant_to_string(const Variant& variant, const cctz::time_zone& zone,
                                                              ColumnBuilder<TYPE_VARCHAR>& result) {
    switch (variant.type()) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
    case VariantType::STRING: {
        auto str = variant.get_string();
        if (!str.ok()) {
            return str.status();
        }

        Slice slice(str.value().data(), str.value().size());
        result.append(slice);
        return Status::OK();
    }
    default: {
        const VariantValue value = VariantValue::of_variant(variant);
        std::stringstream ss;
        Status status = VariantUtil::variant_to_json(value.get_metadata(), value.get_value(), ss, zone);
        if (!status.ok()) {
            return status;
        }

        std::string json_str = ss.str();
        result.append(Slice(json_str));
        return Status::OK();
    }
    }
}

template <LogicalType DecimalType>
StatusOr<RunTimeColumnType<DecimalType>> cast_variant_to_decimal(const Variant& variant,
                                                                 ColumnBuilder<DecimalType>& result) {
    switch (variant.type()) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
    case VariantType::DECIMAL4: {
        auto decimal_value = variant.get_decimal4();
        if (!decimal_value.ok()) {
            return decimal_value.status();
        }

        result.append(static_cast<RunTimeCppType<DecimalType>>(decimal_value.value()));
        return Status::OK();
    }
    case VariantType::DECIMAL8: {
        auto decimal_value = variant.get_decimal8();
        if (!decimal_value.ok()) {
            return decimal_value.status();
        }

        result.append(static_cast<RunTimeCppType<DecimalType>>(decimal_value.value()));
        return Status::OK();
    }
    case VariantType::DECIMAL16: {
        auto decimal_value = variant.get_decimal16();
        if (!decimal_value.ok()) {
            return decimal_value.status();
        }

        result.append(static_cast<RunTimeCppType<DecimalType>>(decimal_value.value()));
        return Status::OK();
    }
    default: {
        return VARIANT_CAST_NOT_SUPPORT(DecimalType);
    }
    }
}

StatusOr<RunTimeColumnType<TYPE_DATETIME>> cast_variant_to_datetime(const Variant& variant, const cctz::time_zone& zone,
                                                                    ColumnBuilder<TYPE_DATETIME>& result) {
    switch (variant.type()) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
    case VariantType::INT8:
    case VariantType::INT16:
    case VariantType::INT32:
    case VariantType::INT64: {
        auto int_value = VariantUtil::get_long(&variant);
        if (!int_value.ok()) {
            return int_value.status();
        }

        int64_t sec = int_value.value();
        TimestampValue tv;
        tv.from_unix_second(sec, 0);
        result.append(std::move(tv));
        return Status::OK();
    }
    case VariantType::TIMESTAMP_TZ: {
        auto timestamp_micros = variant.get_timestamp_micros();
        if (!timestamp_micros.ok()) {
            return timestamp_micros.status();
        }

        const int64_t micros = timestamp_micros.value();
        TimestampValue tv;
        tv.from_unixtime(micros / 1000000, micros % 1000000, zone);
        result.append(std::move(tv));
        return Status::OK();
    }
    case VariantType::TIMESTAMP_NTZ: {
        auto timestamp_micros_ntz = variant.get_timestamp_micros_ntz();
        if (!timestamp_micros_ntz.ok()) {
            return timestamp_micros_ntz.status();
        }

        const int64_t micros = timestamp_micros_ntz.value();
        TimestampValue tv;
        tv.from_unix_second(micros / 1000000, micros % 1000000);
        result.append(std::move(tv));
        return Status::OK();
    }
    case VariantType::STRING: {
        auto str_value = variant.get_string();
        if (!str_value.ok()) {
            return str_value.status();
        }

        const std::string_view str = str_value.value();
        TimestampValue tv{};
        bool ret = tv.from_string(str.data(), str.size());
        if (!ret) {
            return Status::VariantError(
                    fmt::format("Failed to cast string '{}' to DATETIME", std::string(str.data(), str.size())));
        }

        result.append(std::move(tv));
        return Status::OK();
    }
    default: {
        return VARIANT_CAST_NOT_SUPPORT(TYPE_DATETIME);
    }
    }
}

} // namespace starrocks