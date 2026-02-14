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

#include <string>
#include <string_view>
#include <vector>

#include "base/status.h"
#include "base/statusor.h"
#include "types/variant_row_value.h"

namespace starrocks {

// Segment for internal shredded/overlay path representation.
struct VariantOverlayPathSegment {
    bool is_array = false;
    int array_index = -1;
    std::string key;
};

// Parse internal path format used by typed shredded keys and overlay paths:
// - Object: "a.b.c"
// - Array:  "a[0].b"
// - Root:   "" (empty path)
// Returns false when syntax is invalid.
bool parse_variant_overlay_path(std::string_view path, std::vector<VariantOverlayPathSegment>* segments);

// Mutable builder for row-level VARIANT binary.
//
// Why this exists:
// - Shredded read paths need to rebuild one VariantRowValue from:
//   1) base remain payload (optional),
//   2) typed overlays (path -> value),
//   3) metadata dictionary semantics.
// - Variant binary is compacted blob format, unlike JSON in-memory trees.
// - This builder is the future convergence point for metadata-aware overlay merge.
//   Current implementation uses binary decode -> overlay -> binary encode.
class VariantBuilder {
public:
    struct Overlay {
        std::string path;
        VariantRowValue value;
    };

    VariantBuilder() = default;
    ~VariantBuilder() = default;

    // Initialize base row. base == nullptr means start from empty root.
    VariantBuilder& init_from_base(const VariantRowValue* base);

    // Replace overlays in one batch. Builder is not designed for incremental updates.
    Status set_overlays(std::vector<Overlay> overlays);

    // Build the final row-level variant.
    StatusOr<VariantRowValue> build() const;

private:
    const VariantRowValue* _base = nullptr;
    std::vector<Overlay> _overlays;
};

} // namespace starrocks
