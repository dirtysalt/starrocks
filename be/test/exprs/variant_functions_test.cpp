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

#include <glog/logging.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/variant_encoder.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/mock_vectorized_expr.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/variant_base.h"

namespace starrocks {

class VariantFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

    TExprNode expr_node;
};

// Helper function to read variant test data from parquet test files
static std::pair<std::string, std::string> load_variant_test_data(const std::string& metadata_file,
                                                                  const std::string& value_file) {
    FileSystem* fs = FileSystem::Default();

    std::string starrocks_home = getenv("STARROCKS_HOME");
    static std::string test_exec_dir = starrocks_home + "/be/test/exec";
    static std::string variant_test_data_dir = starrocks_home + "/be/test/formats/parquet/test_data/variant/";

    auto metadata_path = variant_test_data_dir + metadata_file;
    auto value_path = variant_test_data_dir + value_file;

    auto metadata_file_obj = *fs->new_random_access_file(metadata_path);
    auto value_file_obj = *fs->new_random_access_file(value_path);

    std::string metadata_content = *metadata_file_obj->read_all();
    std::string value_content = *value_file_obj->read_all();

    return {std::move(metadata_content), std::move(value_content)};
}

// Helper function to create VariantRowValue from test data files
static void create_variant_from_test_data(const std::string& metadata_file, const std::string& value_file,
                                          VariantRowValue& variant) {
    auto [metadata, value] = load_variant_test_data(metadata_file, value_file);
    variant = VariantRowValue(metadata, value);
}

static VariantRowValue create_variant_from_json_text(const std::string& json_text) {
    auto json = JsonValue::parse(json_text);
    CHECK(json.ok()) << json.status().to_string();
    auto encoded = VariantEncoder::encode_json_to_variant(json.value());
    CHECK(encoded.ok()) << encoded.status().to_string();
    return encoded.value();
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

static MutableColumnPtr build_nullable_bool_column(const std::vector<uint8_t>& values,
                                                   const std::vector<uint8_t>& is_null) {
    auto data = BooleanColumn::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i] != 0);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_double_column(const std::vector<double>& values,
                                                     const std::vector<uint8_t>& is_null) {
    auto data = DoubleColumn::create();
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

static MutableColumnPtr build_nullable_variant_column(const std::vector<VariantRowValue>& values,
                                                      const std::vector<uint8_t>& is_null) {
    auto data = VariantColumn::create();
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

static MutableColumnPtr build_nullable_map_si_column(const std::vector<DatumMap>& values,
                                                     const std::vector<uint8_t>& is_null) {
    TypeDescriptor map_type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT));
    auto col = ColumnHelper::create_column(map_type, true);
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

static MutableColumnPtr build_nullable_struct_is_column(const std::vector<DatumStruct>& values,
                                                        const std::vector<uint8_t>& is_null) {
    TypeDescriptor struct_type =
            TypeDescriptor::create_struct_type({"x", "y"}, {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR)});
    auto col = ColumnHelper::create_column(struct_type, true);
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

// Test cases using real variant test data
class VariantQueryTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string, std::string>> {};

// Validates end-to-end variant_query semantics against real parquet variant fixtures.
TEST_P(VariantQueryTestFixture, variant_query_with_test_data) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    std::string metadata_file = std::get<0>(GetParam());
    std::string value_file = std::get<1>(GetParam());
    std::string param_path = std::get<2>(GetParam());
    std::string param_result = std::get<3>(GetParam());

    VariantRowValue variant;

    create_variant_from_test_data(metadata_file, value_file, variant);
    VLOG(10) << "Loaded variant value from test data: " << variant.to_string();
    variant_column->append(variant);

    if (param_path == "NULL") {
        path_builder.append_null();
    } else {
        path_builder.append(param_path);
    }

    Columns columns{variant_column, path_builder.build(true)};
    ctx->set_constant_columns(columns);
    std::ignore =
            VariantFunctions::variant_segments_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone("-04:00", ctz);

    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto variant_result = datum.get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json(ctz);
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        ASSERT_EQ(param_result, variant_str);
    }

    ASSERT_TRUE(VariantFunctions::variant_segments_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test cases using real variant test data from parquet test files
INSTANTIATE_TEST_SUITE_P(
        VariantQueryTestWithRealData, VariantQueryTestFixture,
        ::testing::Values(
                // clang-format off
                // Basic primitive tests using real test data
                std::make_tuple("primitive_boolean_true.metadata", "primitive_boolean_true.value", "$", "true"),
                std::make_tuple("primitive_boolean_false.metadata", "primitive_boolean_false.value", "$", "false"),
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "$", "42"),
                std::make_tuple("primitive_int16.metadata", "primitive_int16.value", "$", "1234"),
                std::make_tuple("primitive_int32.metadata", "primitive_int32.value", "$", "123456"),
                std::make_tuple("primitive_int64.metadata", "primitive_int64.value", "$", "1234567890123456789"),
                std::make_tuple("primitive_float.metadata", "primitive_float.value", "$", "1.23456794e+09"),
                std::make_tuple("primitive_double.metadata", "primitive_double.value", "$", "1234567890.1234"),
                std::make_tuple("primitive_decimal4.metadata", "primitive_decimal4.value", "$", "12.34"),
                std::make_tuple("primitive_decimal8.metadata", "primitive_decimal8.value", "$", "12345678.9"),
                std::make_tuple("primitive_decimal16.metadata", "primitive_decimal16.value", "$", "12345678912345678.9"),
                std::make_tuple("short_string.metadata", "short_string.value", "$", "\"Less than 64 bytes (❤️ with utf8)\""),
                std::make_tuple("primitive_string.metadata", "primitive_string.value", "$", "\"This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!\""),
                std::make_tuple("primitive_timestamp.metadata", "primitive_timestamp.value", "$", "\"2025-04-16 12:34:56.78-04:00\""),
                std::make_tuple("primitive_timestampntz.metadata", "primitive_timestampntz.value", "$", "\"2025-04-16 12:34:56.780000\""),
                std::make_tuple("primitive_date.metadata", "primitive_date.value", "$", "\"2025-04-16\""),

                // Object and array tests
                std::make_tuple("object_primitive.metadata", "object_primitive.value", "$.int_field", "1"),
                std::make_tuple("object_nested.metadata", "object_nested.value", "$.observation.location", "\"In the Volcano\""),
                std::make_tuple("array_primitive.metadata", "array_primitive.value", "$[0]", "2"),
                std::make_tuple("array_nested.metadata", "array_nested.value", "$[0].thing.names[0]", "\"Contrarian\""),

                // Non-existent path tests
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "$.nonexistent", "NULL"),
                std::make_tuple("primitive_string.metadata", "primitive_string.value", "$.missing", "NULL"),

                // Null path tests
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "NULL", "NULL")
                // clang-format on
                ));

// Simplified test cases for basic functionality using simple variant values
class VariantQuerySimpleTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string>> {};

// Validates argument-count contract of variant_query.
TEST_F(VariantFunctionsTest, variant_query_invalid_arguments) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Test with no arguments
    {
        Columns columns;
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }

    // Test with one argument
    {
        auto variant_column = VariantColumn::create();
        Columns columns{variant_column};
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }

    // Test with three arguments
    {
        auto variant_column = VariantColumn::create();
        auto path_column = BinaryColumn::create();
        auto extra_column = BinaryColumn::create();
        Columns columns{variant_column, path_column, extra_column};
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }
}

// Validates null-propagation when both variant input and path input are nullable-null.
TEST_F(VariantFunctionsTest, variant_query_null_columns) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Test with all null columns
    auto variant_column = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    auto path_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());

    variant_column->append_nulls(2);
    path_column->append_nulls(2);

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(2, result->size());
    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
}

// Validates invalid json-path handling returns NULL instead of failing evaluation.
TEST_F(VariantFunctionsTest, variant_query_invalid_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    auto path_column = BinaryColumn::create();

    VariantRowValue variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", variant);
    variant_column->append(variant);

    // Invalid path syntax
    path_column->append("$.invalid..path");

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());
    ASSERT_TRUE(result->is_null(0));
}

// Validates basic object path extraction through variant_query on non-shredded rows.
TEST_F(VariantFunctionsTest, variant_query_complex_types) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    VariantRowValue variant;
    create_variant_from_test_data("object_primitive.metadata", "object_primitive.value", variant);
    variant_column->append(variant);
    path_builder.append("$.int_field");

    Columns columns{variant_column, path_builder.build(true)};
    ctx->set_constant_columns(columns);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());

    // Handle potential ConstColumn wrapping
    Datum datum = result->get(0);
    ASSERT_FALSE(datum.is_null());

    // For TYPE_VARIANT result, the datum should contain a VariantRowValue pointer
    auto variant_result = datum.get_variant();
    ASSERT_TRUE(!!variant_result);
    auto json_result = variant_result->to_json();
    ASSERT_TRUE(json_result.ok());
    const std::string& variant_str = json_result.value();
    ASSERT_EQ("1", variant_str);
}

// Validates per-row dynamic path evaluation across multiple variant rows.
TEST_F(VariantFunctionsTest, variant_query_multiple_rows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    // Create multiple variant values using test data
    std::vector<std::pair<std::string, std::string>> test_files = {
            {"primitive_int8.metadata", "primitive_int8.value"},
            {"primitive_boolean_true.metadata", "primitive_boolean_true.value"},
            {"short_string.metadata", "short_string.value"}};

    std::vector<VariantRowValue> variants;
    variants.reserve(test_files.size());

    for (size_t i = 0; i < test_files.size(); ++i) {
        const auto& [metadata_file, value_file] = test_files[i];
        VariantRowValue variant;
        create_variant_from_test_data(metadata_file, value_file, variant);
        variants.push_back(variant);
        variant_column->append(variant);
        path_builder.append("$");
    }

    Columns columns{variant_column, path_builder.build(true)};
    ctx->set_constant_columns(columns);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(3, result->size());

    std::vector<std::string> expected_results = {"42", "true", "\"Less than 64 bytes (❤️ with utf8)\""};
    for (size_t i = 0; i < 3; ++i) {
        auto variant_result = result->get(i).get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        const std::string& variant_str = json_result.value();
        ASSERT_EQ(expected_results[i], variant_str);
    }
}

// Validates const variant + const path handling in variant_query (const folding path).
TEST_F(VariantFunctionsTest, variant_query_const_columns) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    // Create variant value using test data
    VariantRowValue variant;
    create_variant_from_test_data("short_string.metadata", "short_string.value", variant);
    variant_column->append(variant);
    path_builder.append("$");

    // Create const columns
    auto const_variant = ConstColumn::create(variant_column, 3);
    auto const_path = path_builder.build(true);

    Columns columns{const_variant, const_path};
    ctx->set_constant_columns(columns);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(3, result->size());

    for (size_t i = 0; i < 3; ++i) {
        auto variant_result = result->get(i).get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        const std::string& variant_str = json_result.value();
        ASSERT_EQ("\"Less than 64 bytes (❤️ with utf8)\"", variant_str);
    }
}

// Verifies typed-column direct hit path in _do_variant_query -> try_append_typed_match_result for BIGINT.
TEST_F(VariantFunctionsTest, get_variant_int_shredded_typed_only_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.typed_only");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ(777, result.value()->get(0).get_int64());
}

// Verifies const-path fast path with shredded typed BIGINT across multiple rows.
TEST_F(VariantFunctionsTest, get_variant_int_shredded_const_path_multi_rows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777, 888}, {0, 0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_data = BinaryColumn::create();
    path_data->append("$.typed_only");
    auto const_path = ConstColumn::create(std::move(path_data), 2);

    Columns columns{variant_column, const_path};
    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(2, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_FALSE(result.value()->is_null(1));
    ASSERT_EQ(777, result.value()->get(0).get_int64());
    ASSERT_EQ(888, result.value()->get(1).get_int64());
}

// Verifies typed object-prefix match ("a.b") followed by suffix seek (".int_field").
TEST_F(VariantFunctionsTest, get_variant_int_shredded_object_suffix_path_from_typed_variant) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    VariantRowValue typed_obj_variant;
    create_variant_from_test_data("object_primitive.metadata", "object_primitive.value", typed_obj_variant);

    MutableColumns typed;
    typed.emplace_back(build_nullable_variant_column({typed_obj_variant}, {0}));
    variant_column->set_shredded_columns({"a.b"}, {TypeDescriptor(TYPE_VARIANT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.a.b.int_field");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ(1, result.value()->get(0).get_int64());
}

// Verifies fallback to remain row when typed path does not exist.
TEST_F(VariantFunctionsTest, get_variant_int_shredded_fallback_to_remain_when_typed_path_missing) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("object_primitive.metadata", "object_primitive.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.int_field");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ(1, result.value()->get(0).get_int64());
}

// Verifies fallback to remain row when typed column exists but current typed value is null.
TEST_F(VariantFunctionsTest, get_variant_int_shredded_fallback_to_remain_when_typed_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("object_primitive.metadata", "object_primitive.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({0}, {1}));
    variant_column->set_shredded_columns({"int_field"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.int_field");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ(1, result.value()->get(0).get_int64());
}

// Verifies array-index suffix seek from typed variant payload (e.g. "$.a.c[0]").
TEST_F(VariantFunctionsTest, get_variant_int_shredded_array_index_path_from_typed_variant) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    VariantRowValue typed_array_variant;
    create_variant_from_test_data("array_primitive.metadata", "array_primitive.value", typed_array_variant);

    MutableColumns typed;
    typed.emplace_back(build_nullable_variant_column({typed_array_variant}, {0}));
    variant_column->set_shredded_columns({"a.c"}, {TypeDescriptor(TYPE_VARIANT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.a.c[0]");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ(2, result.value()->get(0).get_int64());
}

// Verifies typed-column direct hit path for VARCHAR without remain fallback.
TEST_F(VariantFunctionsTest, get_variant_string_shredded_typed_only_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_varchar_column({"typed_string"}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.typed_only");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_string(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ("typed_string", result.value()->get(0).get_slice().to_string());
}

// Verifies typed-column direct hit path for BOOLEAN without remain fallback.
TEST_F(VariantFunctionsTest, get_variant_bool_shredded_typed_only_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_bool_column({1}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BOOLEAN)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.typed_only");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_bool(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_TRUE(result.value()->get(0).get_uint8());
}

// Verifies typed-column direct hit path for DOUBLE without remain fallback.
TEST_F(VariantFunctionsTest, get_variant_double_shredded_typed_only_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_double_column({3.5}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_DOUBLE)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.typed_only");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_double(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_DOUBLE_EQ(3.5, result.value()->get(0).get_double());
}

// Verifies VARCHAR read falls back to remain when typed value is null.
TEST_F(VariantFunctionsTest, get_variant_string_shredded_fallback_to_remain_when_typed_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("object_primitive.metadata", "object_primitive.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({0}, {1}));
    variant_column->set_shredded_columns({"int_field"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.int_field");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_string(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ("1", result.value()->get(0).get_slice().to_string());
}

// Verifies BOOLEAN read falls back to remain when typed path is missing.
TEST_F(VariantFunctionsTest, get_variant_bool_shredded_fallback_to_remain_when_typed_path_missing) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_boolean_true.metadata", "primitive_boolean_true.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({0}, {1}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_bool(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_TRUE(result.value()->get(0).get_uint8());
}

// Verifies DOUBLE read falls back to remain when typed value is null.
TEST_F(VariantFunctionsTest, get_variant_double_shredded_fallback_to_remain_when_typed_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("object_primitive.metadata", "object_primitive.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({0}, {1}));
    variant_column->set_shredded_columns({"int_field"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.int_field");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_double(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_DOUBLE_EQ(1.0, result.value()->get(0).get_double());
}

// Verifies variant_query returns typed BIGINT value as VARIANT when typed path is directly hit.
TEST_F(VariantFunctionsTest, variant_query_shredded_typed_only_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.typed_only");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::variant_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    auto variant_result = result.value()->get(0).get_variant();
    ASSERT_NE(nullptr, variant_result);
    auto json_result = variant_result->to_json();
    ASSERT_TRUE(json_result.ok());
    ASSERT_EQ("777", json_result.value());
}

// Verifies const variant + const path combination uses row=0 correctly in shredded mode for all output rows.
TEST_F(VariantFunctionsTest, variant_query_shredded_const_variant_and_const_path_multi_rows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto const_variant = ConstColumn::create(std::move(variant_column), 3);
    auto path_data = BinaryColumn::create();
    path_data->append("$.typed_only");
    auto const_path = ConstColumn::create(std::move(path_data), 3);

    Columns columns{const_variant, const_path};
    auto result = VariantFunctions::variant_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(3, result.value()->size());
    for (size_t i = 0; i < 3; ++i) {
        ASSERT_FALSE(result.value()->is_null(i));
        auto variant_result = result.value()->get(i).get_variant();
        ASSERT_NE(nullptr, variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        ASSERT_EQ("777", json_result.value());
    }
}

// Verifies root typed-only scalar typeof derives type from typed column and keeps null rows null.
TEST_F(VariantFunctionsTest, variant_typeof_root_typed_only_scalar) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777, 0}, {0, 1}));
    variant_column->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    Columns columns{variant_column};
    auto result = VariantFunctions::variant_typeof(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(2, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ("Int64", result.value()->get(0).get_slice().to_string());
    ASSERT_TRUE(result.value()->is_null(1));
}

// Verifies root typed-only ARRAY typeof returns "Array".
TEST_F(VariantFunctionsTest, variant_typeof_root_typed_only_array) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    MutableColumns typed;
    typed.emplace_back(build_nullable_int_array_column({DatumArray{Datum(int64_t(1)), Datum(int64_t(2))}}, {0}));
    variant_column->set_shredded_columns({""}, {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT))},
                                         std::move(typed), nullptr, nullptr);

    Columns columns{variant_column};
    auto result = VariantFunctions::variant_typeof(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ("Array", result.value()->get(0).get_slice().to_string());
}

// Verifies multi-key typed-only typeof is OBJECT at root.
TEST_F(VariantFunctionsTest, variant_typeof_multi_key_typed_only_object) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({1}, {0}));
    typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    variant_column->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARCHAR)},
                                         std::move(typed), nullptr, nullptr);

    Columns columns{variant_column};
    auto result = VariantFunctions::variant_typeof(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ("Object", result.value()->get(0).get_slice().to_string());
}

// Verifies variant_query("$") can directly hit root typed-only scalar path (key="").
TEST_F(VariantFunctionsTest, variant_query_root_typed_only_scalar_root_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777}, {0}));
    variant_column->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    auto path_column = BinaryColumn::create();
    path_column->append("$");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::variant_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    auto variant_result = result.value()->get(0).get_variant();
    ASSERT_NE(nullptr, variant_result);
    auto json_result = variant_result->to_json();
    ASSERT_TRUE(json_result.ok());
    ASSERT_EQ("777", json_result.value());
}

// Verifies root typed-only ARRAY supports suffix seek for "$[i]" path.
TEST_F(VariantFunctionsTest, get_variant_int_root_typed_only_array_index) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    MutableColumns typed;
    typed.emplace_back(build_nullable_int_array_column({DatumArray{Datum(int64_t(1)), Datum(int64_t(2))}}, {0}));
    variant_column->set_shredded_columns({""}, {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT))},
                                         std::move(typed), nullptr, nullptr);

    auto path_column = BinaryColumn::create();
    path_column->append("$[1]");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
    ASSERT_EQ(2, result.value()->get(0).get_int64());
}

// Verifies root typed-only ARRAY out-of-range index returns NULL instead of error.
TEST_F(VariantFunctionsTest, get_variant_int_root_typed_only_array_index_out_of_range_returns_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    MutableColumns typed;
    typed.emplace_back(build_nullable_int_array_column({DatumArray{Datum(int64_t(1)), Datum(int64_t(2))}}, {0}));
    variant_column->set_shredded_columns({""}, {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT))},
                                         std::move(typed), nullptr, nullptr);

    auto path_column = BinaryColumn::create();
    path_column->append("$[9]");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_TRUE(result.value()->is_null(0));
}

// Verifies scalar path type mismatch (indexing scalar) returns NULL.
TEST_F(VariantFunctionsTest, get_variant_int_root_typed_only_scalar_index_type_mismatch_returns_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777}, {0}));
    variant_column->set_shredded_columns({""}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    auto path_column = BinaryColumn::create();
    path_column->append("$[0]");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_TRUE(result.value()->is_null(0));
}

// Verifies typed hit with incompatible result type returns NULL when remain cannot satisfy path.
TEST_F(VariantFunctionsTest, get_variant_int_typed_path_string_type_mismatch_returns_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", remain_variant);
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_varchar_column({"typed_string"}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.typed_only");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_TRUE(result.value()->is_null(0));
}

// Verifies typed-hit cast failure does not fallback to remain on same path (current locked behavior).
TEST_F(VariantFunctionsTest, get_variant_int_typed_cast_fail_does_not_fallback_to_remain_same_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto remain_variant = create_variant_from_json_text(R"({"typed_only":123})");
    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    MutableColumns typed;
    typed.emplace_back(build_nullable_varchar_column({"typed_string"}, {0}));
    variant_column->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_column = BinaryColumn::create();
    path_column->append("$.typed_only");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_TRUE(result.value()->is_null(0));
}

// Verifies missing path and explicit JSON null are both surfaced as SQL NULL.
TEST_F(VariantFunctionsTest, get_variant_int_missing_and_json_null_both_return_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    variant_column->append(create_variant_from_json_text(R"({"a":null})"));
    variant_column->append(create_variant_from_json_text(R"({"b":1})"));

    auto path_column = BinaryColumn::create();
    path_column->append("$.a");
    path_column->append("$.a");
    Columns columns{variant_column, path_column};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(2, result.value()->size());
    ASSERT_TRUE(result.value()->is_null(0)); // explicit json null
    ASSERT_TRUE(result.value()->is_null(1)); // missing path
}

// Verifies base_shredded + const typed column reads use typed row 0 across rows.
TEST_F(VariantFunctionsTest, get_variant_int_base_shredded_const_typed_column) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    VariantRowValue remain_variant = create_variant_from_json_text(R"({"a":0})");
    auto remain_metadata = remain_variant.get_metadata().raw();
    auto remain_value = remain_variant.get_value().raw();
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    metadata_col->append(Slice(remain_metadata.data(), remain_metadata.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));
    remain_col->append(Slice(remain_value.data(), remain_value.size()));

    auto typed_data = Int64Column::create();
    typed_data->append(42);
    MutableColumns typed;
    typed.emplace_back(ConstColumn::create(std::move(typed_data), 2));
    variant_column->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed),
                                         std::move(metadata_col), std::move(remain_col));

    auto path_data = BinaryColumn::create();
    path_data->append("$.a");
    auto const_path = ConstColumn::create(std::move(path_data), 2);
    Columns columns{variant_column, const_path};

    auto result = VariantFunctions::get_variant_int(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(2, result.value()->size());
    ASSERT_EQ(42, result.value()->get(0).get_int64());
    ASSERT_EQ(42, result.value()->get(1).get_int64());
}

// Verifies root typed-only MAP can be queried at root and represented as variant OBJECT.
TEST_F(VariantFunctionsTest, variant_query_root_typed_only_map_root_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    DatumMap m;
    m[(Slice) "k1"] = (int32_t)1;
    m[(Slice) "k2"] = (int32_t)2;
    MutableColumns typed;
    typed.emplace_back(build_nullable_map_si_column({m}, {0}));
    variant_column->set_shredded_columns(
            {""}, {TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT))},
            std::move(typed), nullptr, nullptr);

    auto path_column = BinaryColumn::create();
    path_column->append("$");
    Columns columns{variant_column, path_column};
    auto result = VariantFunctions::variant_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
}

// Verifies root typed-only STRUCT can be queried at root and represented as variant OBJECT.
TEST_F(VariantFunctionsTest, variant_query_root_typed_only_struct_root_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();

    DatumStruct s{Datum(int32_t(7)), Datum("x")};
    MutableColumns typed;
    typed.emplace_back(build_nullable_struct_is_column({s}, {0}));
    variant_column->set_shredded_columns(
            {""},
            {TypeDescriptor::create_struct_type({"x", "y"}, {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR)})},
            std::move(typed), nullptr, nullptr);

    auto path_column = BinaryColumn::create();
    path_column->append("$");
    Columns columns{variant_column, path_column};
    auto result = VariantFunctions::variant_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));
}

} // namespace starrocks
