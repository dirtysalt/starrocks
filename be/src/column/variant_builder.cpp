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

#include <arrow/util/endian.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
namespace starrocks {

bool parse_variant_overlay_path(std::string_view path, std::vector<VariantOverlayPathSegment>* segments) {
    if (segments == nullptr) {
        return false;
    }
    segments->clear();

    size_t i = 0;
    while (i < path.size()) {
        if (path[i] == '.') {
            ++i;
            continue;
        }
        if (path[i] == '[') {
            ++i;
            if (i >= path.size() || !std::isdigit(static_cast<unsigned char>(path[i]))) {
                return false;
            }
            int value = 0;
            while (i < path.size() && std::isdigit(static_cast<unsigned char>(path[i]))) {
                value = value * 10 + (path[i] - '0');
                ++i;
            }
            if (i >= path.size() || path[i] != ']') {
                return false;
            }
            ++i;
            segments->push_back(VariantOverlayPathSegment{.is_array = true, .array_index = value, .key = ""});
            continue;
        }

        size_t start = i;
        while (i < path.size() && path[i] != '.' && path[i] != '[') {
            ++i;
        }
        if (i == start) {
            return false;
        }
        segments->push_back(VariantOverlayPathSegment{
                .is_array = false, .array_index = -1, .key = std::string(path.substr(start, i - start))});
    }

    return true;
}

namespace variant_builder_internal {

enum class NodeKind : uint8_t {
    kNull = 0,
    kScalar = 1,
    kObject = 2,
    kArray = 3,
};

struct VariantNode {
    NodeKind kind = NodeKind::kNull;
    std::string scalar_raw;
    std::vector<std::pair<std::string, std::unique_ptr<VariantNode>>> fields;
    std::unordered_map<std::string, size_t> field_index;
    std::vector<std::unique_ptr<VariantNode>> elements;

    void set_null() {
        kind = NodeKind::kNull;
        scalar_raw.clear();
        fields.clear();
        field_index.clear();
        elements.clear();
    }

    void set_scalar(std::string raw) {
        kind = NodeKind::kScalar;
        scalar_raw = std::move(raw);
        fields.clear();
        field_index.clear();
        elements.clear();
    }

    void set_object() {
        if (kind == NodeKind::kObject) {
            return;
        }
        kind = NodeKind::kObject;
        scalar_raw.clear();
        fields.clear();
        field_index.clear();
        elements.clear();
    }

    void set_array() {
        if (kind == NodeKind::kArray) {
            return;
        }
        kind = NodeKind::kArray;
        scalar_raw.clear();
        fields.clear();
        field_index.clear();
        elements.clear();
    }

    VariantNode* find_or_insert_field(std::string_view key) {
        auto it = field_index.find(std::string(key));
        if (it != field_index.end()) {
            return fields[it->second].second.get();
        }
        const size_t idx = fields.size();
        fields.emplace_back(std::string(key), std::make_unique<VariantNode>());
        field_index.emplace(fields.back().first, idx);
        return fields.back().second.get();
    }
};

static Status decode_variant_to_node(const VariantMetadata& metadata, const VariantValue& value, VariantNode* node) {
    if (node == nullptr) {
        return Status::InvalidArgument("decode target node is null");
    }
    const VariantType type = value.type();
    if (type == VariantType::NULL_TYPE) {
        node->set_null();
        return Status::OK();
    }
    if (type == VariantType::OBJECT) {
        ASSIGN_OR_RETURN(const auto info, value.get_object_info());
        const std::string_view raw = value.raw();
        node->set_object();
        node->fields.reserve(info.num_elements);
        node->field_index.reserve(info.num_elements);
        for (uint32_t i = 0; i < info.num_elements; ++i) {
            const uint32_t field_id = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.id_start_offset + i * info.id_size, info.id_size);
            ASSIGN_OR_RETURN(const auto key, metadata.get_key(field_id));
            const uint32_t offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + i * info.offset_size, info.offset_size);
            const uint32_t next_offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + (i + 1) * info.offset_size, info.offset_size);
            if (next_offset < offset || info.data_start_offset + next_offset > raw.size()) {
                return Status::VariantError("Invalid variant object field offset");
            }
            VariantNode child;
            RETURN_IF_ERROR(decode_variant_to_node(
                    metadata, VariantValue(raw.substr(info.data_start_offset + offset, next_offset - offset)), &child));
            const size_t idx = node->fields.size();
            node->fields.emplace_back(std::string(key), std::make_unique<VariantNode>(std::move(child)));
            node->field_index.emplace(node->fields.back().first, idx);
        }
        return Status::OK();
    }
    if (type == VariantType::ARRAY) {
        ASSIGN_OR_RETURN(const auto info, value.get_array_info());
        const std::string_view raw = value.raw();
        node->set_array();
        node->elements.resize(info.num_elements);
        for (uint32_t i = 0; i < info.num_elements; ++i) {
            const uint32_t offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + i * info.offset_size, info.offset_size);
            const uint32_t next_offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + (i + 1) * info.offset_size, info.offset_size);
            if (next_offset < offset || info.data_start_offset + next_offset > raw.size()) {
                return Status::VariantError("Invalid variant array element offset");
            }
            auto child = std::make_unique<VariantNode>();
            RETURN_IF_ERROR(decode_variant_to_node(
                    metadata, VariantValue(raw.substr(info.data_start_offset + offset, next_offset - offset)),
                    child.get()));
            node->elements[i] = std::move(child);
        }
        return Status::OK();
    }

    node->set_scalar(std::string(value.raw()));
    return Status::OK();
}

template <typename T>
static void append_little_endian(std::string* out, T value) {
    const T le_value = arrow::bit_util::ToLittleEndian(value);
    out->append(reinterpret_cast<const char*>(&le_value), sizeof(T));
}

static void append_uint_le(std::string* out, uint32_t value, uint8_t size) {
    const uint32_t le_value = arrow::bit_util::ToLittleEndian(value);
    out->append(reinterpret_cast<const char*>(&le_value), size);
}

static uint8_t minimal_uint_size(uint32_t value) {
    if (value <= 0xFF) {
        return 1;
    }
    if (value <= 0xFFFF) {
        return 2;
    }
    if (value <= 0xFFFFFF) {
        return 3;
    }
    return 4;
}

static std::string encode_null_value() {
    return std::string(VariantValue::kEmptyValue);
}

static std::string encode_array_from_elements(const std::vector<std::string>& elements) {
    const uint32_t num_elements = static_cast<uint32_t>(elements.size());
    const bool is_large = num_elements > 255;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint32_t total_data_size = 0;
    for (const auto& element : elements) {
        total_data_size += static_cast<uint32_t>(element.size());
    }

    const uint8_t offset_size = minimal_uint_size(total_data_size);
    uint8_t vheader = static_cast<uint8_t>((offset_size - 1) | (static_cast<uint8_t>(is_large) << 2));
    char header = static_cast<char>((vheader << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::ARRAY));

    std::string out;
    out.reserve(1 + num_elements_size + (num_elements + 1) * offset_size + total_data_size);
    out.push_back(header);
    append_uint_le(&out, num_elements, num_elements_size);

    uint32_t offset = 0;
    append_uint_le(&out, offset, offset_size);
    for (const auto& element : elements) {
        offset += static_cast<uint32_t>(element.size());
        append_uint_le(&out, offset, offset_size);
    }
    for (const auto& element : elements) {
        out.append(element.data(), element.size());
    }
    return out;
}

static std::string encode_object_from_fields(const std::map<uint32_t, std::string>& fields) {
    const uint32_t num_elements = static_cast<uint32_t>(fields.size());
    const bool is_large = num_elements > 255;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint32_t max_field_id = 0;
    uint32_t total_data_size = 0;
    for (const auto& [field_id, value] : fields) {
        max_field_id = std::max(max_field_id, field_id);
        total_data_size += static_cast<uint32_t>(value.size());
    }

    const uint8_t field_id_size = minimal_uint_size(max_field_id);
    const uint8_t field_offset_size = minimal_uint_size(total_data_size);
    uint8_t vheader = static_cast<uint8_t>((field_offset_size - 1) | ((field_id_size - 1) << 2) |
                                           (static_cast<uint8_t>(is_large) << 4));
    char header = static_cast<char>((vheader << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::OBJECT));

    std::string out;
    out.reserve(1 + num_elements_size + num_elements * field_id_size + (num_elements + 1) * field_offset_size +
                total_data_size);
    out.push_back(header);
    append_uint_le(&out, num_elements, num_elements_size);
    for (const auto& [field_id, _] : fields) {
        append_uint_le(&out, field_id, field_id_size);
    }

    uint32_t offset = 0;
    append_uint_le(&out, offset, field_offset_size);
    for (const auto& [_, value] : fields) {
        offset += static_cast<uint32_t>(value.size());
        append_uint_le(&out, offset, field_offset_size);
    }
    for (const auto& [_, value] : fields) {
        out.append(value.data(), value.size());
    }
    return out;
}

static void collect_object_keys(const VariantNode& node, std::unordered_set<std::string>* keys) {
    if (node.kind == NodeKind::kObject) {
        for (const auto& [key, child] : node.fields) {
            keys->emplace(key);
            collect_object_keys(*child, keys);
        }
        return;
    }
    if (node.kind == NodeKind::kArray) {
        for (const auto& element : node.elements) {
            if (element != nullptr) {
                collect_object_keys(*element, keys);
            }
        }
    }
}

struct VariantMetadataBuildResult {
    std::string metadata;
    std::unordered_map<std::string, uint32_t> key_to_id;
};

static StatusOr<VariantMetadataBuildResult> build_variant_metadata(const std::unordered_set<std::string>& keys) {
    VariantMetadataBuildResult result;
    if (keys.empty()) {
        result.metadata.assign(VariantMetadata::kEmptyMetadata.data(), VariantMetadata::kEmptyMetadata.size());
        return result;
    }

    std::vector<std::string> sorted_keys;
    sorted_keys.reserve(keys.size());
    for (const auto& key : keys) {
        sorted_keys.emplace_back(key);
    }
    std::sort(sorted_keys.begin(), sorted_keys.end());

    uint32_t total_string_size = 0;
    for (const auto& key : sorted_keys) {
        total_string_size += static_cast<uint32_t>(key.size());
    }
    const uint32_t dict_size = static_cast<uint32_t>(sorted_keys.size());
    const uint8_t offset_size = minimal_uint_size(std::max(dict_size, total_string_size));

    uint8_t header = 1; // version
    header |= 0b10000;  // sorted/unique
    header |= static_cast<uint8_t>((offset_size - 1) << 6);

    result.metadata.reserve(1 + offset_size * (dict_size + 2) + total_string_size);
    result.metadata.push_back(static_cast<char>(header));
    append_uint_le(&result.metadata, dict_size, offset_size);

    uint32_t offset = 0;
    append_uint_le(&result.metadata, offset, offset_size);
    for (const auto& key : sorted_keys) {
        offset += static_cast<uint32_t>(key.size());
        append_uint_le(&result.metadata, offset, offset_size);
    }

    for (uint32_t i = 0; i < dict_size; ++i) {
        result.key_to_id.emplace(sorted_keys[i], i);
        result.metadata.append(sorted_keys[i].data(), sorted_keys[i].size());
    }
    return result;
}

static StatusOr<std::string> encode_node_value(const VariantNode& node,
                                               const std::unordered_map<std::string, uint32_t>& dict_indexes) {
    if (node.kind == NodeKind::kNull) {
        return encode_null_value();
    }
    if (node.kind == NodeKind::kScalar) {
        return node.scalar_raw;
    }
    if (node.kind == NodeKind::kArray) {
        std::vector<std::string> elements;
        elements.reserve(node.elements.size());
        for (const auto& element : node.elements) {
            if (element == nullptr) {
                elements.emplace_back(encode_null_value());
            } else {
                ASSIGN_OR_RETURN(auto value, encode_node_value(*element, dict_indexes));
                elements.emplace_back(std::move(value));
            }
        }
        return encode_array_from_elements(elements);
    }

    std::map<uint32_t, std::string> fields;
    for (const auto& [key, child] : node.fields) {
        auto it = dict_indexes.find(key);
        if (it == dict_indexes.end()) {
            return Status::InvalidArgument("variant mutable builder miss key index: " + key);
        }
        ASSIGN_OR_RETURN(auto value, encode_node_value(*child, dict_indexes));
        fields[it->second] = std::move(value);
    }
    return encode_object_from_fields(fields);
}

static Status apply_overlay(VariantNode* root, std::string_view path, VariantNode overlay) {
    if (root == nullptr) {
        return Status::InvalidArgument("overlay root is null");
    }
    std::vector<VariantOverlayPathSegment> segments;
    if (!parse_variant_overlay_path(path, &segments)) {
        return Status::InvalidArgument("invalid overlay path: " + std::string(path));
    }
    if (segments.empty()) {
        *root = std::move(overlay);
        return Status::OK();
    }

    VariantNode* current = root;
    for (size_t i = 0; i < segments.size(); ++i) {
        const auto& seg = segments[i];
        const bool last = i + 1 == segments.size();
        if (seg.is_array) {
            if (seg.array_index < 0) {
                return Status::InvalidArgument("invalid overlay array index");
            }
            current->set_array();
            const size_t idx = static_cast<size_t>(seg.array_index);
            if (current->elements.size() <= idx) {
                current->elements.resize(idx + 1);
            }
            if (!current->elements[idx]) {
                current->elements[idx] = std::make_unique<VariantNode>();
            }
            if (last) {
                *current->elements[idx] = std::move(overlay);
            } else {
                current = current->elements[idx].get();
            }
        } else {
            current->set_object();
            VariantNode* child = current->find_or_insert_field(seg.key);
            if (last) {
                *child = std::move(overlay);
            } else {
                current = child;
            }
        }
    }
    return Status::OK();
}

} // namespace variant_builder_internal

VariantBuilder& VariantBuilder::init_from_base(const VariantRowValue* base) {
    _base = base;
    return *this;
}

Status VariantBuilder::set_overlays(std::vector<Overlay> overlays) {
    _overlays = std::move(overlays);
    return Status::OK();
}

StatusOr<VariantRowValue> VariantBuilder::build() const {
    using namespace variant_builder_internal;
    VariantNode root;
    bool has_content = false;

    if (_base != nullptr) {
        RETURN_IF_ERROR(decode_variant_to_node(_base->get_metadata(), _base->get_value(), &root));
        has_content = true;
    }

    for (const auto& overlay : _overlays) {
        if (overlay.value.get_value().type() == VariantType::NULL_TYPE) {
            continue;
        }
        VariantNode overlay_node;
        RETURN_IF_ERROR(decode_variant_to_node(overlay.value.get_metadata(), overlay.value.get_value(), &overlay_node));
        RETURN_IF_ERROR(apply_overlay(&root, overlay.path, std::move(overlay_node)));
        has_content = true;
    }

    if (!has_content) {
        return VariantRowValue::from_null();
    }

    std::unordered_set<std::string> keys;
    collect_object_keys(root, &keys);
    ASSIGN_OR_RETURN(auto metadata_result, build_variant_metadata(keys));
    ASSIGN_OR_RETURN(auto value, encode_node_value(root, metadata_result.key_to_id));
    return VariantRowValue::create(metadata_result.metadata, value);
}

} // namespace starrocks
