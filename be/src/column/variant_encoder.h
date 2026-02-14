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

#include <string_view>

#include "base/status.h"
#include "base/statusor.h"
#include "column/column.h"
#include "column/column_builder.h"
#include "types/json_value.h"
#include "types/type_descriptor.h"
#include "types/variant_row_value.h"
#include "velocypack/Slice.h"

namespace starrocks {

class VariantEncoder {
public:
    static Status encode_column(const ColumnPtr& column, const TypeDescriptor& type,
                                ColumnBuilder<TYPE_VARIANT>* builder, bool allow_throw_exception);
    static StatusOr<VariantRowValue> encode_vslice_to_variant(const vpack::Slice& slice);
    static StatusOr<VariantRowValue> encode_json_to_variant(const JsonValue& json);
    static StatusOr<VariantRowValue> encode_json_text_to_variant(std::string_view json_text);
};

} // namespace starrocks
