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

#include "column/variant_builder.h"

#include <gtest/gtest.h>

#include "base/testutil/parallel_test.h"
#include "column/variant_encoder.h"

namespace starrocks {

// Verifies overlays without base can assemble one object row.
PARALLEL_TEST(VariantBuilderTest, build_from_overlays_only) {
    VariantBuilder builder;
    auto a = VariantEncoder::encode_json_text_to_variant("99");
    ASSERT_TRUE(a.ok());
    auto b = VariantEncoder::encode_json_text_to_variant("\"x\"");
    ASSERT_TRUE(b.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a", .value = std::move(a.value())},
            {.path = "b", .value = std::move(b.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":99,"b":"x"})", json.value());
}

// Verifies overlapped paths can be merged in one result tree.
PARALLEL_TEST(VariantBuilderTest, build_with_overlapped_paths) {
    VariantBuilder builder;
    auto a = VariantEncoder::encode_json_text_to_variant(R"({"x":1})");
    ASSERT_TRUE(a.ok());
    auto b = VariantEncoder::encode_json_text_to_variant("2");
    ASSERT_TRUE(b.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a", .value = std::move(a.value())},
            {.path = "a.b", .value = std::move(b.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"b":2,"x":1}})", json.value());
}

// Verifies null overlay keeps base value (skipNull semantics).
PARALLEL_TEST(VariantBuilderTest, null_overlay_keeps_base_value) {
    VariantBuilder base_builder;
    auto base_value = VariantEncoder::encode_json_text_to_variant(R"({"a":1,"b":2})");
    ASSERT_TRUE(base_value.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = "", .value = std::move(base_value.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base = base_builder.build();
    ASSERT_TRUE(base.ok());

    VariantBuilder builder;
    builder.init_from_base(&base.value());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a", .value = VariantRowValue::from_null()},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":1,"b":2})", json.value());
}

// Verifies array-index overlays can build sparse arrays with nested objects.
PARALLEL_TEST(VariantBuilderTest, build_sparse_array_overlay) {
    VariantBuilder builder;
    auto value = VariantEncoder::encode_json_text_to_variant("7");
    ASSERT_TRUE(value.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a[1].b", .value = std::move(value.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":[null,{"b":7}]})", json.value());
}

// Verifies assigning parent path after child path overrides prior subtree.
PARALLEL_TEST(VariantBuilderTest, parent_overlay_overrides_child_overlay) {
    VariantBuilder builder;
    auto child = VariantEncoder::encode_json_text_to_variant("1");
    ASSERT_TRUE(child.ok());
    auto parent = VariantEncoder::encode_json_text_to_variant(R"({"x":2})");
    ASSERT_TRUE(parent.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a.b", .value = std::move(child.value())},
            {.path = "a", .value = std::move(parent.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"x":2}})", json.value());
}

// Verifies object child overlay can overwrite a scalar parent from base row.
PARALLEL_TEST(VariantBuilderTest, object_overlay_replaces_scalar_base) {
    VariantBuilder base_builder;
    auto base_value = VariantEncoder::encode_json_text_to_variant(R"({"a":1})");
    ASSERT_TRUE(base_value.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = "", .value = std::move(base_value.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base = base_builder.build();
    ASSERT_TRUE(base.ok());

    VariantBuilder builder;
    builder.init_from_base(&base.value());
    auto overlay_value = VariantEncoder::encode_json_text_to_variant("3");
    ASSERT_TRUE(overlay_value.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a.b", .value = std::move(overlay_value.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"b":3}})", json.value());
}

// Verifies root path overlay replaces whole root.
PARALLEL_TEST(VariantBuilderTest, root_overlay_replaces_whole_root) {
    VariantBuilder builder;
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"a":1})");
    ASSERT_TRUE(base.ok());
    builder.init_from_base(&base.value());
    auto root = VariantEncoder::encode_json_text_to_variant(R"({"x":[1,2]})");
    ASSERT_TRUE(root.ok());

    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "", .value = std::move(root.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());
    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"x":[1,2]})", json.value());
}

// Verifies overlays on same path use last-wins semantics.
PARALLEL_TEST(VariantBuilderTest, same_path_last_overlay_wins) {
    VariantBuilder builder;
    auto first = VariantEncoder::encode_json_text_to_variant("1");
    ASSERT_TRUE(first.ok());
    auto second = VariantEncoder::encode_json_text_to_variant("2");
    ASSERT_TRUE(second.ok());

    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a", .value = std::move(first.value())},
            {.path = "a", .value = std::move(second.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());
    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":2})", json.value());
}

// Verifies typed variant overlay is remapped by semantic decode/encode, not raw dict-id reuse.
PARALLEL_TEST(VariantBuilderTest, variant_overlay_dict_namespace_remap) {
    VariantBuilder base_builder;
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"p":0})");
    ASSERT_TRUE(base.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = "", .value = std::move(base.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base_row = base_builder.build();
    ASSERT_TRUE(base_row.ok());

    VariantBuilder builder;
    builder.init_from_base(&base_row.value());
    auto overlay_obj = VariantEncoder::encode_json_text_to_variant(R"({"x":1,"y":2})");
    ASSERT_TRUE(overlay_obj.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "a", .value = std::move(overlay_obj.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"x":1,"y":2},"p":0})", json.value());
}

// Verifies base object key order is preserved and new overlay keys append in encounter order.
PARALLEL_TEST(VariantBuilderTest, object_key_order_stable_with_overlay_append) {
    VariantBuilder base_builder;
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"b":1,"a":2})");
    ASSERT_TRUE(base.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = "", .value = std::move(base.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base_row = base_builder.build();
    ASSERT_TRUE(base_row.ok());

    VariantBuilder builder;
    builder.init_from_base(&base_row.value());
    auto c = VariantEncoder::encode_json_text_to_variant("3");
    ASSERT_TRUE(c.ok());
    auto d = VariantEncoder::encode_json_text_to_variant("4");
    ASSERT_TRUE(d.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = "c", .value = std::move(c.value())},
            {.path = "d", .value = std::move(d.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":2,"b":1,"c":3,"d":4})", json.value());
}

// Verifies empty root path is valid and maps to zero segments.
PARALLEL_TEST(VariantBuilderTest, parse_empty_root_path) {
    std::vector<VariantOverlayPathSegment> segments;
    ASSERT_TRUE(parse_variant_overlay_path("", &segments));
    ASSERT_TRUE(segments.empty());
}

// Verifies mixed object/array internal path can be parsed in order.
PARALLEL_TEST(VariantBuilderTest, parse_mixed_object_array_path) {
    std::vector<VariantOverlayPathSegment> segments;
    ASSERT_TRUE(parse_variant_overlay_path("a.b[2].c", &segments));
    ASSERT_EQ(4, segments.size());

    ASSERT_FALSE(segments[0].is_array);
    ASSERT_EQ("a", segments[0].key);

    ASSERT_FALSE(segments[1].is_array);
    ASSERT_EQ("b", segments[1].key);

    ASSERT_TRUE(segments[2].is_array);
    ASSERT_EQ(2, segments[2].array_index);

    ASSERT_FALSE(segments[3].is_array);
    ASSERT_EQ("c", segments[3].key);
}

// Verifies root array path is accepted by overlay parser.
PARALLEL_TEST(VariantBuilderTest, parse_root_array_path) {
    std::vector<VariantOverlayPathSegment> segments;
    ASSERT_TRUE(parse_variant_overlay_path("[0].a", &segments));
    ASSERT_EQ(2, segments.size());

    ASSERT_TRUE(segments[0].is_array);
    ASSERT_EQ(0, segments[0].array_index);

    ASSERT_FALSE(segments[1].is_array);
    ASSERT_EQ("a", segments[1].key);
}

// Verifies unclosed array bracket is rejected.
PARALLEL_TEST(VariantBuilderTest, reject_unclosed_array_bracket) {
    std::vector<VariantOverlayPathSegment> segments;
    ASSERT_FALSE(parse_variant_overlay_path("a[1", &segments));
}

// Verifies non-digit array index is rejected.
PARALLEL_TEST(VariantBuilderTest, reject_nondigit_array_index) {
    std::vector<VariantOverlayPathSegment> segments;
    ASSERT_FALSE(parse_variant_overlay_path("a[x]", &segments));
}

// Verifies negative array index is rejected.
PARALLEL_TEST(VariantBuilderTest, reject_negative_array_index) {
    std::vector<VariantOverlayPathSegment> segments;
    ASSERT_FALSE(parse_variant_overlay_path("a[-1]", &segments));
}

} // namespace starrocks
