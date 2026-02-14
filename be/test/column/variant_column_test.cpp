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

#include "column/variant_column.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string_view>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/variant_encoder.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/variant_base.h"
#include "types/variant_row_value.h"

namespace starrocks {

static inline uint8_t primitive_header(VariantType primitive) {
    return static_cast<uint8_t>(primitive) << 2;
}

static MutableColumnPtr build_nullable_int64_column(const std::vector<int64_t>& values,
                                                    const std::vector<uint8_t>& is_null) {
    auto data = Int64Column::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_varchar_column(const std::vector<std::string>& values,
                                                      const std::vector<uint8_t>& is_null) {
    auto data = BinaryColumn::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_int_array_column(const std::vector<DatumArray>& values,
                                                        const std::vector<uint8_t>& is_null) {
    TypeDescriptor array_type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT));
    auto col = ColumnHelper::create_column(array_type, true);
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        if (is_null[i] != 0) {
            col->append_nulls(1);
        } else {
            col->append_datum(Datum(values[i]));
        }
    }
    return col;
}

static MutableColumnPtr build_nullable_variant_column(const std::vector<std::string>& json_values,
                                                      const std::vector<uint8_t>& is_null) {
    auto data = VariantColumn::create();
    auto null = NullColumn::create();
    DCHECK_EQ(json_values.size(), is_null.size());
    for (size_t i = 0; i < json_values.size(); ++i) {
        if (is_null[i] != 0) {
            VariantRowValue row = VariantRowValue::from_null();
            data->append(&row);
            null->append(1);
            continue;
        }
        auto encoded = VariantEncoder::encode_json_text_to_variant(json_values[i]);
        DCHECK(encoded.ok()) << encoded.status().to_string();
        data->append(&encoded.value());
        null->append(0);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_binary_column(const std::vector<std::string>& values,
                                                     const std::vector<uint8_t>& is_null) {
    auto data = BinaryColumn::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static void append_primitive_int8_row(BinaryColumn* metadata, BinaryColumn* remain, int8_t value) {
    const std::string metadata_bytes(VariantMetadata::kEmptyMetadata);
    const char payload[2] = {static_cast<char>(primitive_header(VariantType::INT8)), static_cast<char>(value)};
    metadata->append(Slice(metadata_bytes.data(), metadata_bytes.size()));
    remain->append(Slice(payload, sizeof(payload)));
}

static auto build_shredded_variant_column_for_ut() {
    auto col = VariantColumn::create();

    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_primitive_int8_row(metadata.get(), remain.get(), 1);
    append_primitive_int8_row(metadata.get(), remain.get(), 2);
    append_primitive_int8_row(metadata.get(), remain.get(), 3);

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({10, 20, 30}, {0, 1, 0}));

    col->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                              std::move(remain));
    return col;
}

static void append_json_variant_row(BinaryColumn* metadata, BinaryColumn* remain, std::string_view json_text) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(json_text);
    ASSERT_TRUE(encoded.ok()) << encoded.status().to_string();
    std::string_view metadata_raw = encoded->get_metadata().raw();
    std::string_view value_raw = encoded->get_value().raw();
    metadata->append(Slice(metadata_raw.data(), metadata_raw.size()));
    remain->append(Slice(value_raw.data(), value_raw.size()));
}

static VariantRowValue create_variant_row_from_json_text(std::string_view json_text) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(json_text);
    DCHECK(encoded.ok()) << encoded.status().to_string();
    return encoded.value();
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_build_column) {
    // create column
    {
        const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};
        auto column = VariantColumn::create();
        EXPECT_EQ("variant", column->get_name());
        EXPECT_TRUE(column->is_variant());
        column->append(&variant);

        const VariantRowValue* res = column->get_object(0);
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());
    }
    // clone
    {
        auto column = VariantColumn::create();
        const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};
        column->append(&variant);

        auto copy = column->clone();
        ASSERT_EQ(copy->size(), 1);

        auto* variant_column = down_cast<VariantColumn*>(copy.get());
        const VariantRowValue* res = variant_column->get_object(0);
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_serialize) {
    std::string_view empty_metadata = VariantMetadata::kEmptyMetadata;
    const uint8_t uuid_chars[] = {primitive_header(VariantType::UUID),
                                  0xf2,
                                  0x4f,
                                  0x9b,
                                  0x64,
                                  0x81,
                                  0xfa,
                                  0x49,
                                  0xd1,
                                  0xb7,
                                  0x4e,
                                  0x8c,
                                  0x09,
                                  0xa6,
                                  0xe3,
                                  0x1c,
                                  0x56};

    std::string_view uuid_string(reinterpret_cast<const char*>(uuid_chars), sizeof(uuid_chars));
    VariantRowValue variant{empty_metadata, uuid_string};

    auto column = VariantColumn::create();
    EXPECT_EQ("variant", column->get_name());
    EXPECT_TRUE(column->is_variant());
    column->append(&variant);
    EXPECT_EQ(variant.serialize_size(), column->serialize_size(0));

    std::vector<uint8_t> buffer;
    buffer.resize(variant.serialize_size());
    column->serialize(0, buffer.data());
    auto new_column = column->clone_empty();
    new_column->deserialize_and_append(buffer.data());
    const VariantRowValue* deserialized_variant = new_column->get(0).get_variant();
    ASSERT_TRUE(deserialized_variant != nullptr);
    EXPECT_EQ(variant.serialize_size(), deserialized_variant->serialize_size());
    EXPECT_EQ(variant.to_string(), deserialized_variant->to_string());
    EXPECT_EQ("\"f24f9b64-81fa-49d1-b74e-8c09a6e31c56\"", deserialized_variant->to_json().value());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, put_mysql_row_buffer) {
    const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
    std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
    VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};

    auto column = VariantColumn::create();
    column->append(&variant);

    MysqlRowBuffer buf;
    column->put_mysql_row_buffer(&buf, 0);
    EXPECT_EQ("\n1234567890", buf.data());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_create_variant_column) {
    auto variant_column = VariantColumn::create();

    EXPECT_EQ(0, variant_column->size());
    EXPECT_TRUE(variant_column->empty());
    EXPECT_FALSE(variant_column->is_nullable());
    EXPECT_FALSE(variant_column->is_constant());

    auto cloned = variant_column->clone();
    EXPECT_EQ(0, cloned->size());

    size_t memory_usage = variant_column->memory_usage();
    EXPECT_GE(memory_usage, 0);
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_append_strings) {
    const auto variant_column = VariantColumn::create();
    const uint8_t int1_value[] = {primitive_header(VariantType::INT8), 0x01};
    const std::string_view int1_value_str(reinterpret_cast<const char*>(int1_value), sizeof(int1_value));
    constexpr uint32_t int1_total_size = sizeof(int1_value) + VariantMetadata::kEmptyMetadata.size();
    std::string variant_string;
    variant_string.resize(int1_total_size + sizeof(uint32_t));
    memcpy(variant_string.data(), &int1_total_size, sizeof(uint32_t));
    memcpy(variant_string.data() + sizeof(uint32_t), VariantMetadata::kEmptyMetadata.data(),
           VariantMetadata::kEmptyMetadata.size());
    memcpy(variant_string.data() + sizeof(uint32_t) + VariantMetadata::kEmptyMetadata.size(), int1_value_str.data(),
           int1_value_str.size());
    const Slice slice(variant_string.data(), variant_string.size());
    variant_column->append_strings(&slice, 1);

    ASSERT_EQ(1, variant_column->size());
    auto expected = VariantRowValue::create(slice);
    ASSERT_TRUE(expected.ok());
    const VariantRowValue* actual = variant_column->get_object(0);
    ASSERT_EQ(expected->serialize_size(), actual->serialize_size());
    ASSERT_EQ(expected->get_metadata(), actual->get_metadata());
    ASSERT_EQ(expected->get_value(), actual->get_value());
    EXPECT_EQ(expected->to_string(), actual->to_string());
    EXPECT_EQ("1", actual->to_string());

    const Slice bad_slice("");
    const bool result = variant_column->append_strings(&bad_slice, 1);
    ASSERT_FALSE(result) << "Appending empty slice should fail";
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_append_interop) {
    auto src = build_shredded_variant_column_for_ut();

    auto dst_append = VariantColumn::create();
    dst_append->append(*src, 0, src->size());
    ASSERT_TRUE(dst_append->is_shredded_variant());
    ASSERT_EQ(3, dst_append->size());

    const auto* typed_append = down_cast<const NullableColumn*>(dst_append->typed_column_by_index(0));
    ASSERT_FALSE(typed_append->get(0).is_null());
    ASSERT_EQ(10, typed_append->get(0).get_int64());
    ASSERT_TRUE(typed_append->get(1).is_null());
    ASSERT_EQ(30, typed_append->get(2).get_int64());

    const auto m0 = dst_append->metadata_column()->get(0).get_slice();
    const auto r0 = dst_append->remain_value_column()->get(0).get_slice();
    ASSERT_EQ(VariantMetadata::kEmptyMetadata.size(), m0.size);
    ASSERT_EQ(2, r0.size);
    ASSERT_EQ(static_cast<char>(primitive_header(VariantType::INT8)), r0.data[0]);
    ASSERT_EQ(1, static_cast<int8_t>(r0.data[1]));

    uint32_t indexes[] = {2, 0};
    auto dst_selective = VariantColumn::create();
    dst_selective->append_selective(*src, indexes, 0, 2);
    ASSERT_TRUE(dst_selective->is_shredded_variant());
    ASSERT_EQ(2, dst_selective->size());

    const auto* typed_selective = down_cast<const NullableColumn*>(dst_selective->typed_column_by_index(0));
    ASSERT_EQ(30, typed_selective->get(0).get_int64());
    ASSERT_EQ(10, typed_selective->get(1).get_int64());

    const auto r_sel0 = dst_selective->remain_value_column()->get(0).get_slice();
    const auto r_sel1 = dst_selective->remain_value_column()->get(1).get_slice();
    ASSERT_EQ(3, static_cast<int8_t>(r_sel0.data[1]));
    ASSERT_EQ(1, static_cast<int8_t>(r_sel1.data[1]));

    auto dst_repeat = VariantColumn::create();
    dst_repeat->append_value_multiple_times(*src, 1, 2);
    ASSERT_TRUE(dst_repeat->is_shredded_variant());
    ASSERT_EQ(2, dst_repeat->size());

    const auto* typed_repeat = down_cast<const NullableColumn*>(dst_repeat->typed_column_by_index(0));
    ASSERT_TRUE(typed_repeat->get(0).is_null());
    ASSERT_TRUE(typed_repeat->get(1).is_null());
    const auto r_rep0 = dst_repeat->remain_value_column()->get(0).get_slice();
    const auto r_rep1 = dst_repeat->remain_value_column()->get(1).get_slice();
    ASSERT_EQ(2, static_cast<int8_t>(r_rep0.data[1]));
    ASSERT_EQ(2, static_cast<int8_t>(r_rep1.data[1]));
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_append_schema_alignment) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 2);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10, 11}, {0, 0})); // a
    src_typed.emplace_back(build_nullable_int64_column({20, 21}, {0, 0})); // b
    src->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                              std::move(src_typed), std::move(src_metadata), std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({200}, {0})); // b
    dst_typed.emplace_back(build_nullable_int64_column({300}, {0})); // c
    dst->set_shredded_columns({"b", "c"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                              std::move(dst_typed), std::move(dst_metadata), std::move(dst_remain));

    dst->append(*src, 0, src->size());

    ASSERT_TRUE(dst->is_shredded_variant());
    ASSERT_EQ(3, dst->size());
    ASSERT_EQ((std::vector<std::string>{"a", "b", "c"}), dst->shredded_paths());

    const auto* typed_a = down_cast<const NullableColumn*>(dst->typed_column_by_index(0));
    const auto* typed_b = down_cast<const NullableColumn*>(dst->typed_column_by_index(1));
    const auto* typed_c = down_cast<const NullableColumn*>(dst->typed_column_by_index(2));

    ASSERT_TRUE(typed_a->get(0).is_null());
    ASSERT_EQ(10, typed_a->get(1).get_int64());
    ASSERT_EQ(11, typed_a->get(2).get_int64());

    ASSERT_EQ(200, typed_b->get(0).get_int64());
    ASSERT_EQ(20, typed_b->get(1).get_int64());
    ASSERT_EQ(21, typed_b->get(2).get_int64());

    ASSERT_EQ(300, typed_c->get(0).get_int64());
    ASSERT_TRUE(typed_c->get(1).is_null());
    ASSERT_TRUE(typed_c->get(2).is_null());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_schema_alignment_type_conflict) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 2);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    ASSERT_FALSE(dst->align_shredded_schema_with(*src));
    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());
    ASSERT_EQ(1, dst->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_schema_alignment_metadata_presence_mismatch) {
    auto src = VariantColumn::create();
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10, 11}, {0, 0}));
    src->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), nullptr, nullptr);

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 8);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({20, 21}, {0, 0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    ASSERT_FALSE(dst->align_shredded_schema_with(*src));
    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());
    ASSERT_EQ(2, dst->size());
}

// Verifies root typed-only scalar rows are materialized via typed column when metadata/remain are absent.
PARALLEL_TEST(VariantColumnTest, test_root_typed_only_scalar_materialization) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777, 0}, {0, 1}));
    col->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    ASSERT_TRUE(col->is_shredded_variant());
    ASSERT_TRUE(col->is_typed_only_variant());
    ASSERT_TRUE(col->is_root_typed_only_variant());
    ASSERT_FALSE(col->has_metadata_column());
    ASSERT_FALSE(col->has_remain_value());
    ASSERT_EQ(2, col->size());

    VariantRowValue row0;
    const VariantRowValue* v0 = col->get_row_value(0, &row0);
    ASSERT_NE(nullptr, v0);
    auto json0 = v0->to_json();
    ASSERT_TRUE(json0.ok());
    ASSERT_EQ("777", json0.value());

    VariantRowValue row1;
    const VariantRowValue* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v1);
    auto json1 = v1->to_json();
    ASSERT_TRUE(json1.ok());
    ASSERT_EQ("null", json1.value());

    EXPECT_GT(col->serialize_size(0), 0);
    EXPECT_GT(col->serialize_size(1), 0);
}

// Verifies root typed-only supports const typed column row access (row index should map to 0 for const data).
PARALLEL_TEST(VariantColumnTest, test_root_typed_only_const_typed_column) {
    auto col = VariantColumn::create();
    auto typed_data = Int64Column::create();
    typed_data->append(42);
    MutableColumns typed;
    typed.emplace_back(ConstColumn::create(std::move(typed_data), 3));
    col->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    ASSERT_TRUE(col->is_root_typed_only_variant());
    ASSERT_EQ(3, col->size());
    for (size_t row = 0; row < 3; ++row) {
        VariantRowValue row_buffer;
        const VariantRowValue* v = col->get_row_value(row, &row_buffer);
        ASSERT_NE(nullptr, v);
        auto json = v->to_json();
        ASSERT_TRUE(json.ok());
        ASSERT_EQ("42", json.value());
    }
}

// Verifies root typed-only ARRAY rows can be materialized through get_row_value().
PARALLEL_TEST(VariantColumnTest, test_root_typed_only_array_materialization) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int_array_column({DatumArray{Datum(int64_t(1)), Datum(int64_t(2))}}, {0}));
    col->set_shredded_columns({""}, {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT))}, std::move(typed),
                              nullptr, nullptr);

    ASSERT_TRUE(col->is_root_typed_only_variant());
    ASSERT_TRUE(col->is_typed_only_variant());

    VariantRowValue row;
    const VariantRowValue* v = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, v);
    auto json = v->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ("[1,2]", json.value());
}

// Verifies multi-key typed-only rows are materialized as root OBJECT without metadata/remain.
PARALLEL_TEST(VariantColumnTest, test_multi_key_typed_only_object_materialization) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({10}, {0}));
    typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    col->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARCHAR)}, std::move(typed),
                              nullptr, nullptr);

    ASSERT_TRUE(col->is_shredded_variant());
    ASSERT_TRUE(col->is_typed_only_variant());
    ASSERT_FALSE(col->is_root_typed_only_variant());
    ASSERT_FALSE(col->has_metadata_column());
    ASSERT_FALSE(col->has_remain_value());

    VariantRowValue row;
    const VariantRowValue* v = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, v);
    auto json = v->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ("{\"a\":10,\"b\":\"x\"}", json.value());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_append_fallback_on_alignment_failure) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append(*src, 0, 1);

    ASSERT_EQ(2, dst->size());
    const auto* typed_a = down_cast<const NullableColumn*>(dst->typed_column_by_index(0));
    ASSERT_EQ("x", typed_a->get(0).get_slice().to_string());
    ASSERT_TRUE(typed_a->get(1).is_null());
}

// Verifies append_selective also falls back to row append when shredded schema alignment fails.
PARALLEL_TEST(VariantColumnTest, test_shredded_append_selective_fallback_on_alignment_failure) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    uint32_t indexes[] = {0};
    dst->append_selective(*src, indexes, 0, 1);

    ASSERT_EQ(2, dst->size());
    const auto* typed_a = down_cast<const NullableColumn*>(dst->typed_column_by_index(0));
    ASSERT_EQ("x", typed_a->get(0).get_slice().to_string());
    ASSERT_TRUE(typed_a->get(1).is_null());
}

// Verifies append_value_multiple_times also falls back to row append when shredded schema alignment fails.
PARALLEL_TEST(VariantColumnTest, test_shredded_append_value_multiple_times_fallback_on_alignment_failure) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append_value_multiple_times(*src, 0, 2);

    ASSERT_EQ(3, dst->size());
    const auto* typed_a = down_cast<const NullableColumn*>(dst->typed_column_by_index(0));
    ASSERT_EQ("x", typed_a->get(0).get_slice().to_string());
    ASSERT_TRUE(typed_a->get(1).is_null());
    ASSERT_TRUE(typed_a->get(2).is_null());
}

// Verifies base_shredded row reconstruction merges typed overlays with base remain payload.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_overrides_remain) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({"a":1,"b":2})");

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({99}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":99,"b":2})", json.value());
}

// Verifies base_shredded reconstruction keeps remain value when typed overlay is NULL.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_null_fallback_remain) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({"a":1,"b":2})");

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({0}, {1}));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":1,"b":2})", json.value());
}

// Verifies typed TYPE_VARIANT overlays with independent metadata are re-encoded into destination base metadata.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_variant_overlay) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({"a":0,"b":2})");

    MutableColumns typed;
    typed.emplace_back(build_nullable_variant_column({R"({"x":1,"y":"z"})"}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor::create_variant_type()}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"x":1,"y":"z"},"b":2})", json.value());
}

// Verifies TYPE_VARIANT typed overlays remap by key-string (not raw dict-id) when source/target metadata namespaces differ.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_variant_overlay_dict_namespace_remap) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({"a":0,"zzz":9,"k":2})");

    // Source typed variant has its own local metadata dictionary where "k" is field-id 0.
    // Target row metadata dictionary field-id 0 is for "a", so raw id reuse would be wrong.
    MutableColumns typed;
    typed.emplace_back(build_nullable_variant_column({R"({"k":1})"}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor::create_variant_type()}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_TRUE(json.value() == R"({"a":{"k":1},"zzz":9,"k":2})" || json.value() == R"({"a":{"k":1},"k":2,"zzz":9})");
}

// Verifies duplicate shredded paths are rejected as invalid schema.
PARALLEL_TEST(VariantColumnTest, test_shredded_schema_reject_duplicate_paths) {
#ifdef NDEBUG
    GTEST_SKIP() << "DCHECK-based schema validation is disabled in release mode";
#else
    EXPECT_DEATH(
            {
                auto col = VariantColumn::create();
                MutableColumns typed;
                typed.emplace_back(build_nullable_int64_column({1}, {0}));
                typed.emplace_back(build_nullable_int64_column({2}, {0}));
                col->set_shredded_columns({"a", "a"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                                          std::move(typed), nullptr, nullptr);
            },
            "Invalid shredded schema");
#endif
}

// Verifies typed_only -> base_shredded promotion keeps historical rows owned by typed columns only.
PARALLEL_TEST(VariantColumnTest, test_typed_only_promotion_preserves_historical_rows_with_null_base) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({7, 8}, {0, 0}));
    col->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    ASSERT_TRUE(col->is_typed_only_variant());
    ASSERT_FALSE(col->has_metadata_column());
    ASSERT_FALSE(col->has_remain_value());

    VariantRowValue appended = create_variant_row_from_json_text(R"({"x":1})");
    col->append(&appended);

    ASSERT_TRUE(col->is_shredded_variant());
    ASSERT_TRUE(col->has_metadata_column());
    ASSERT_TRUE(col->has_remain_value());
    ASSERT_EQ(3, col->size());

    VariantRowValue row0;
    VariantRowValue row1;
    VariantRowValue row2;
    auto* v0 = col->get_row_value(0, &row0);
    auto* v1 = col->get_row_value(1, &row1);
    auto* v2 = col->get_row_value(2, &row2);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    ASSERT_NE(nullptr, v2);
    auto j0 = v0->to_json();
    auto j1 = v1->to_json();
    auto j2 = v2->to_json();
    ASSERT_TRUE(j0.ok());
    ASSERT_TRUE(j1.ok());
    ASSERT_TRUE(j2.ok());
    ASSERT_EQ("7", j0.value());
    ASSERT_EQ("8", j1.value());
    ASSERT_EQ(R"({"x":1})", j2.value());

    VariantRowValue null_base = VariantRowValue::from_null();
    std::string_view null_meta = null_base.get_metadata().raw();
    std::string_view null_value = null_base.get_value().raw();
    auto m0 = col->metadata_column()->get(0).get_slice();
    auto r0 = col->remain_value_column()->get(0).get_slice();
    auto m1 = col->metadata_column()->get(1).get_slice();
    auto r1 = col->remain_value_column()->get(1).get_slice();
    ASSERT_EQ(null_meta.size(), m0.size);
    ASSERT_EQ(null_value.size(), r0.size);
    ASSERT_EQ(null_meta.size(), m1.size);
    ASSERT_EQ(null_value.size(), r1.size);
    ASSERT_EQ(0, memcmp(null_meta.data(), m0.data, m0.size));
    ASSERT_EQ(0, memcmp(null_value.data(), r0.data, r0.size));
    ASSERT_EQ(0, memcmp(null_meta.data(), m1.data, m1.size));
    ASSERT_EQ(0, memcmp(null_value.data(), r1.data, r1.size));
}

// Verifies promotion appends new rows as remain payload while typed overlay for new rows stays null.
PARALLEL_TEST(VariantColumnTest, test_typed_only_promotion_new_rows_use_remain_payload) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({42}, {0}));
    col->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    VariantRowValue appended = create_variant_row_from_json_text(R"({"k":"v"})");
    col->append(&appended);

    ASSERT_EQ(2, col->size());
    const auto* typed_root = down_cast<const NullableColumn*>(col->typed_column_by_index(0));
    ASSERT_FALSE(typed_root->get(0).is_null());
    ASSERT_TRUE(typed_root->get(1).is_null());

    VariantRowValue row0;
    VariantRowValue row1;
    auto* v0 = col->get_row_value(0, &row0);
    auto* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    auto j0 = v0->to_json();
    auto j1 = v1->to_json();
    ASSERT_TRUE(j0.ok());
    ASSERT_TRUE(j1.ok());
    ASSERT_EQ("42", j0.value());
    ASSERT_EQ(R"({"k":"v"})", j1.value());
}

// Verifies base_shredded rows with null metadata/remain are treated as NULL rows even when typed exists.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_null_metadata_or_remain_row_returns_null) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"x":1})");
    std::string metadata(base.get_metadata().raw());
    std::string value(base.get_value().raw());

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({7, 8}, {0, 0}));
    auto metadata_col = build_nullable_binary_column({metadata, metadata}, {0, 1});
    auto remain_col = build_nullable_binary_column({value, value}, {0, 0});
    col->set_shredded_columns({"x"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata_col),
                              std::move(remain_col));

    VariantRowValue row0;
    VariantRowValue row1;
    const VariantRowValue* v0 = col->get_row_value(0, &row0);
    const VariantRowValue* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_EQ(nullptr, v1);
}

// Verifies base_shredded with const typed column uses row=0 typed value for all rows.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_const_typed_column_materialization) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"a":0})");
    std::string metadata(base.get_metadata().raw());
    std::string value(base.get_value().raw());

    auto typed_data = Int64Column::create();
    typed_data->append(42);
    MutableColumns typed;
    typed.emplace_back(ConstColumn::create(std::move(typed_data), 2));
    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    metadata_col->append(metadata);
    metadata_col->append(metadata);
    remain_col->append(value);
    remain_col->append(value);
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata_col),
                              std::move(remain_col));

    VariantRowValue row0;
    VariantRowValue row1;
    auto* v0 = col->get_row_value(0, &row0);
    auto* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    auto j0 = v0->to_json();
    auto j1 = v1->to_json();
    ASSERT_TRUE(j0.ok());
    ASSERT_TRUE(j1.ok());
    ASSERT_EQ(R"({"a":42})", j0.value());
    ASSERT_EQ(R"({"a":42})", j1.value());
}

} // namespace starrocks
