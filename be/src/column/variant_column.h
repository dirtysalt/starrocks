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

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/status.h"
#include "column/column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"
#include "types/variant_row_value.h"

namespace starrocks {

class VariantColumn final
        : public CowFactory<ColumnFactory<ObjectColumn<VariantRowValue>, VariantColumn>, VariantColumn, Column> {
public:
    using ValueType = VariantRowValue;
    using SuperClass = CowFactory<ColumnFactory<ObjectColumn<VariantRowValue>, VariantColumn>, VariantColumn, Column>;
    using BaseClass = VariantColumnBase;
    using ImmContainer = ObjectDataProxyContainer;

    VariantColumn() = default;
    explicit VariantColumn(size_t size) : SuperClass(size) {}
    DISALLOW_COPY(VariantColumn);

    VariantColumn(VariantColumn&& rhs) noexcept
            : SuperClass(std::move(rhs)),
              _shredded_paths(std::move(rhs._shredded_paths)),
              _shredded_types(std::move(rhs._shredded_types)),
              _typed_columns(std::move(rhs._typed_columns)),
              _metadata_column(std::move(rhs._metadata_column)),
              _remain_value_column(std::move(rhs._remain_value_column)) {}

    MutableColumnPtr clone() const override;
    MutableColumnPtr clone_empty() const override { return this->create(); }

    uint32_t serialize(size_t idx, uint8_t* pos) const override;
    uint32_t serialize_size(size_t idx) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void append_datum(const Datum& datum) override;
    void append(const Column& src, size_t offset, size_t count) override;
    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;
    void append_value_multiple_times(const void* value, size_t count) override;
    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    // Add a forwarding function to expose the base class append function
    void append(const Column& src) { append(src, 0, src.size()); }
    void append(const VariantRowValue* object);
    // Keep reference overload for generic append paths (for example ColumnBuilder<TYPE_VARIANT>).
    void append(const VariantRowValue& object);
    void append_shredded(Slice metadata, Slice remain_value);
    void append_shredded_null();
    bool append_nulls(size_t count) override;
    void append_default() override;
    void append_default(size_t count) override;

    size_t size() const override;
    size_t capacity() const override;
    size_t byte_size(size_t from, size_t size) const override;
    void resize(size_t n) override;
    void assign(size_t n, size_t idx) override;
    size_t filter_range(const Filter& filter, size_t from, size_t to) override;
    void swap_column(Column& rhs) override;
    void reset_column() override;
    void check_or_die() const override;

    bool is_variant() const override { return true; }
    // A VariantColumn is considered shredded if it has typed columns,
    // or has both metadata/remain columns.
    bool is_shredded_variant() const;
    // Typed-only shredded shape:
    // - typed columns exist
    // - metadata/remain are absent
    bool is_typed_only_variant() const;
    // Root typed-only shape:
    // - shredded_paths == {""}
    // - typed_columns.size() == 1
    // - metadata/remain are absent
    bool is_root_typed_only_variant() const;
    const VariantRowValue* get_row_value(size_t idx, VariantRowValue* output) const;
    bool try_materialize_row(size_t idx, VariantRowValue* output) const;

    std::string get_name() const override { return "variant"; }

    void set_shredded_columns(std::vector<std::string> paths, std::vector<TypeDescriptor> type_descs,
                              MutableColumns columns, MutableColumnPtr metadata_column,
                              MutableColumnPtr remain_value_column);

    void clear_shredded_columns();

    const std::vector<std::string>& shredded_paths() const { return _shredded_paths; }

    const std::vector<TypeDescriptor>& shredded_types() const { return _shredded_types; }

    std::vector<TypeDescriptor>& mutable_shredded_types() { return _shredded_types; }

    const MutableColumns& typed_columns() const { return _typed_columns; }

    MutableColumns& mutable_typed_columns() { return _typed_columns; }
    int find_shredded_path(std::string_view path) const;
    const Column* typed_column_by_index(size_t idx) const;

    const MutableColumnPtr& metadata_column() const { return _metadata_column; }

    const MutableColumnPtr& remain_value_column() const { return _remain_value_column; }

    bool has_metadata_column() const { return _metadata_column != nullptr; }

    bool has_remain_value() const { return _remain_value_column != nullptr; }

    bool is_equal_schema(const VariantColumn* other) const;

    // Initialize this column as shredded mode using schema from `other`.
    // This requires `other` to be shredded and `this` to be empty.
    Status init_shredded_schema_from(const VariantColumn& other);

    // Prepare destination schema for appending from `src` in shredded mode.
    // - If `src` is row mode, this is a no-op and returns true.
    // - If `src` is shredded and `this` is row mode, initialize shredded schema from `src`.
    // - If both are shredded, align schema when needed.
    // Returns false on unrecoverable alignment/precondition failure.
    bool prepare_shredded_schema_for_append(const VariantColumn& src);

    // Align destination shredded schema with source shredded schema before append fast path.
    //
    // In shredded mode, schema is index-aligned:
    //   shredded_paths[i] <-> shredded_types[i](TypeDescriptor) <-> typed_columns[i]
    //
    // This function normalizes `this` column into a canonical order for append operations:
    //   1) source paths first (same order as `src`),
    //   2) then destination-only paths (keep current destination order).
    //
    // Missing destination paths will be created as nullable typed columns and backfilled
    // with nulls to preserve row alignment. For same path with incompatible logical types,
    // this function returns false.
    bool align_shredded_schema_with(const VariantColumn& src);

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;

private:
    void _init_container_schema_from(const VariantColumn& other);
    void _append_container_rows(const VariantColumn& src, size_t offset, size_t count);
    void _append_container_rows_selective(const VariantColumn& src, const uint32_t* indexes, uint32_t from,
                                          uint32_t count);

    bool _is_shredded_schema_valid() const;

    bool _is_shredded_row_aligned() const;

    size_t _shredded_num_rows() const;
    bool _promote_typed_only_to_base_shredded();

private:
    std::vector<std::string> _shredded_paths;
    std::vector<TypeDescriptor> _shredded_types;
    MutableColumns _typed_columns;
    MutableColumnPtr _metadata_column;
    MutableColumnPtr _remain_value_column;
};

} // namespace starrocks
