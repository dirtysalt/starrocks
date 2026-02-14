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

#include "variant_column.h"

#include <cctz/time_zone.h>

#include <unordered_map>
#include <unordered_set>

#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "column/variant_builder.h"
#include "column/variant_encoder.h"
#include "gutil/casts.h"
#include "types/variant_row_value.h"

namespace starrocks {

// Typed-only variant stores data in typed columns. Read paths still need row-level VariantRowValue.
// These helpers keep reconstruction logic local to VariantColumn and shared by get_row_value/serialize/merge-promotion.
static StatusOr<VariantRowValue> encode_typed_cell_as_variant_row(const Column* typed_column, size_t typed_row,
                                                                  const TypeDescriptor& type_desc) {
    if (typed_column == nullptr) {
        return Status::InvalidArgument("typed column is null");
    }
    if (typed_column->is_null(typed_row)) {
        return VariantRowValue::from_null();
    }
    Datum datum = typed_column->get(typed_row);
    if (datum.is_null()) {
        return VariantRowValue::from_null();
    }

    if (type_desc.type == TYPE_VARIANT) {
        const VariantRowValue* variant = datum.get_variant();
        if (variant == nullptr) {
            return Status::InvalidArgument("Typed variant datum is null");
        }
        // IMPORTANT: Variant object field IDs are local to its own metadata dictionary.
        // We do not directly merge binary payloads from typed columns into destination rows.
        // VariantBuilder rebuilds the final row from semantic values and re-encodes
        // with a unified metadata dictionary, so foreign dict IDs are not leaked.
        return *variant;
    }

    auto one_row_typed_col = typed_column->clone_empty();
    one_row_typed_col->append(*typed_column, typed_row, 1);

    ColumnBuilder<TYPE_VARIANT> variant_builder(1);
    RETURN_IF_ERROR(VariantEncoder::encode_column(one_row_typed_col, type_desc, &variant_builder, false));
    auto encoded_variant_col = variant_builder.build(false);

    const auto* encoded_data_column =
            down_cast<const VariantColumn*>(ColumnHelper::get_data_column(encoded_variant_col.get()));
    if (encoded_data_column == nullptr || encoded_data_column->size() != 1) {
        return Status::InvalidArgument("Failed to encode typed datum to variant row");
    }
    const VariantRowValue* encoded_row = encoded_data_column->get_object(0);
    if (encoded_row == nullptr) {
        return Status::InvalidArgument("Failed to encode typed datum to variant row");
    }
    return *encoded_row;
}

static bool collect_typed_overlays(const VariantColumn* column, size_t row,
                                   std::vector<VariantBuilder::Overlay>* overlays) {
    if (column == nullptr || overlays == nullptr) {
        return false;
    }
    overlays->clear();
    overlays->reserve(column->typed_columns().size());
    for (size_t i = 0; i < column->typed_columns().size(); ++i) {
        const Column* typed_column = column->typed_column_by_index(i);
        if (typed_column == nullptr) {
            return false;
        }
        size_t typed_row = typed_column->is_constant() ? 0 : row;
        auto typed_variant = encode_typed_cell_as_variant_row(typed_column, typed_row, column->shredded_types()[i]);
        if (!typed_variant.ok()) {
            return false;
        }
        overlays->emplace_back(VariantBuilder::Overlay{
                .path = column->shredded_paths()[i],
                .value = std::move(typed_variant.value()),
        });
    }
    return true;
}

static bool rebuild_row_with_optional_base(const VariantRowValue* base, std::vector<VariantBuilder::Overlay> overlays,
                                           VariantRowValue* output) {
    if (output == nullptr) {
        return false;
    }
    VariantBuilder builder;
    if (base != nullptr) {
        builder.init_from_base(base);
    }
    if (!builder.set_overlays(std::move(overlays)).ok()) {
        return false;
    }
    auto encoded = builder.build();
    if (!encoded.ok()) {
        return false;
    }
    *output = std::move(encoded.value());
    return true;
}

static bool rebuild_row_from_typed_columns(const VariantColumn* column, size_t row, VariantRowValue* output) {
    if (column == nullptr || output == nullptr || !column->is_typed_only_variant()) {
        return false;
    }
    std::vector<VariantBuilder::Overlay> overlays;
    if (!collect_typed_overlays(column, row, &overlays)) {
        return false;
    }
    return rebuild_row_with_optional_base(nullptr, std::move(overlays), output);
}

static bool rebuild_row_from_base_shredded(const VariantColumn* column, size_t row, VariantRowValue* output,
                                           std::string_view metadata_raw, std::string_view remain_raw) {
    if (column == nullptr || output == nullptr) {
        return false;
    }
    // Base shredded payload may come from legacy/external sources.
    // Avoid eager metadata validation here; defer to downstream decode/seek semantics.
    VariantRowValue base_row(metadata_raw, remain_raw);

    std::vector<VariantBuilder::Overlay> overlays;
    if (!collect_typed_overlays(column, row, &overlays)) {
        return false;
    }
    return rebuild_row_with_optional_base(&base_row, std::move(overlays), output);
}

MutableColumnPtr VariantColumn::clone() const {
    auto cloned = BaseClass::clone();
    auto* variant_cloned = down_cast<VariantColumn*>(cloned.get());
    if (is_shredded_variant()) {
        variant_cloned->_shredded_paths = _shredded_paths;
        variant_cloned->_shredded_types = _shredded_types;
        variant_cloned->_typed_columns.reserve(_typed_columns.size());
        for (const auto& column : _typed_columns) {
            variant_cloned->_typed_columns.emplace_back(column->clone());
        }
        if (_metadata_column != nullptr) {
            variant_cloned->_metadata_column = _metadata_column->clone();
        }
        if (_remain_value_column != nullptr) {
            variant_cloned->_remain_value_column = _remain_value_column->clone();
        }
    }
    return cloned;
}

uint32_t VariantColumn::serialize(size_t idx, uint8_t* pos) const {
    if (is_shredded_variant()) {
        VariantRowValue row;
        if (get_row_value(idx, &row) == nullptr) {
            row = VariantRowValue::from_null();
        }
        return static_cast<uint32_t>(row.serialize(pos));
    }
    return static_cast<uint32_t>(get_object(idx)->serialize(pos));
}

void VariantColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                    uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

uint32_t VariantColumn::serialize_size(size_t idx) const {
    if (is_shredded_variant()) {
        VariantRowValue row;
        if (get_row_value(idx, &row) == nullptr) {
            row = VariantRowValue::from_null();
        }
        return static_cast<uint32_t>(row.serialize_size());
    }
    return static_cast<uint32_t>(get_object(idx)->serialize_size());
}

const uint8_t* VariantColumn::deserialize_and_append(const uint8_t* pos) {
    // Read the first 4 bytes to get the size of the variant
    uint32_t variant_length = *reinterpret_cast<const uint32_t*>(pos);
    auto variant_result = VariantRowValue::create(Slice(pos, variant_length + sizeof(uint32_t)));

    if (!variant_result.ok()) {
        throw std::runtime_error("Failed to deserialize variant: " + variant_result.status().to_string());
    }

    uint32_t serialize_size = variant_result->serialize_size();
    VariantRowValue value = std::move(variant_result.value());
    append(&value);

    return pos + serialize_size;
}

int VariantColumn::find_shredded_path(std::string_view path) const {
    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        if (_shredded_paths[i] == path) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

const Column* VariantColumn::typed_column_by_index(size_t idx) const {
    if (idx >= _typed_columns.size()) {
        return nullptr;
    }
    return _typed_columns[idx].get();
}

void VariantColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    VariantRowValue row;
    const VariantRowValue* variant = get_row_value(idx, &row);
    DCHECK(variant != nullptr) << "Variant value is null at index " << idx;
    if (variant == nullptr) {
        buf->push_null();
        return;
    }
    auto json = variant->to_json();
    if (!json.ok()) {
        buf->push_null();
    } else {
        buf->push_string(json->data(), json->size(), '\'');
    }
}

void VariantColumn::append_datum(const Datum& datum) {
    if (is_shredded_variant()) {
        auto* value = datum.get<VariantRowValue*>();
        append(value);
        return;
    }
    BaseClass::append(datum.get<VariantRowValue*>());
}

void VariantColumn::append(const Column& src, size_t offset, size_t count) {
    size_t before_size = size();
    const auto* other = down_cast<const VariantColumn*>(&src);
    if (!is_shredded_variant() && !other->is_shredded_variant()) {
        BaseClass::append(src, offset, count);
        DCHECK_EQ(size(), before_size + count);
        return;
    }

    if (other->is_shredded_variant()) {
        bool prepared = prepare_shredded_schema_for_append(*other);
        if (!prepared) {
            LOG(WARNING) << "failed to align shredded schema for append, fallback to row append";
            DCHECK(is_shredded_variant());
            for (size_t i = 0; i < count; ++i) {
                VariantRowValue row_buffer;
                const VariantRowValue* row = other->get_row_value(offset + i, &row_buffer);
                append(row);
            }
            DCHECK_EQ(size(), before_size + count);
            return;
        }
        DCHECK(is_equal_schema(other) || _typed_columns.size() >= other->_typed_columns.size());
        _append_container_rows(*other, offset, count);
        DCHECK_EQ(size(), before_size + count);
        return;
    }

    // Source is row-wise, destination is shredded: append row by row.
    DCHECK(is_shredded_variant());
    for (size_t i = 0; i < count; ++i) {
        auto* row = other->get_object(offset + i);
        append(row);
    }
    size_t appended = size() - before_size;
    if (UNLIKELY(appended != count)) {
        if (appended < count) {
            append_nulls(count - appended);
        } else {
            resize(before_size + count);
        }
    }
    DCHECK_EQ(size(), before_size + count);
}

void VariantColumn::append_value_multiple_times(const void* value, size_t count) {
    if (!is_shredded_variant()) {
        BaseClass::append_value_multiple_times(value, count);
        return;
    }
    const auto* row = reinterpret_cast<const VariantRowValue*>(value);
    for (size_t i = 0; i < count; ++i) {
        append(row);
    }
}

void VariantColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t count) {
    const auto* other = down_cast<const VariantColumn*>(&src);
    if (!is_shredded_variant() && !other->is_shredded_variant()) {
        BaseClass::append_selective(src, indexes, from, count);
        return;
    }

    if (is_shredded_variant() && other->is_shredded_variant()) {
        bool prepared = prepare_shredded_schema_for_append(*other);
        if (!prepared) {
            LOG(WARNING) << "failed to align shredded schema for append_selective, fallback to row append";
            for (size_t i = from; i < from + count; ++i) {
                VariantRowValue row_buffer;
                const VariantRowValue* row = other->get_row_value(indexes[i], &row_buffer);
                append(row);
            }
            return;
        }
        DCHECK(is_equal_schema(other) || _typed_columns.size() >= other->_typed_columns.size());
        _append_container_rows_selective(*other, indexes, from, count);
        return;
    }

    if (other->is_shredded_variant()) {
        // Destination was row mode and has just been initialized into shredded mode.
        bool prepared = prepare_shredded_schema_for_append(*other);
        DCHECK(prepared);
        if (!prepared) {
            LOG(WARNING) << "failed to align shredded schema for append_selective, fallback to row append";
            for (size_t i = from; i < from + count; ++i) {
                VariantRowValue row_buffer;
                const VariantRowValue* row = other->get_row_value(indexes[i], &row_buffer);
                append(row);
            }
            return;
        }
        _append_container_rows_selective(*other, indexes, from, count);
        return;
    }

    DCHECK(is_shredded_variant());
    for (size_t i = from; i < from + count; ++i) {
        auto* row = other->get_object(indexes[i]);
        append(row);
    }
}

void VariantColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t count) {
    const auto* other = down_cast<const VariantColumn*>(&src);
    if (!is_shredded_variant() && !other->is_shredded_variant()) {
        BaseClass::append_value_multiple_times(src, index, count);
        return;
    }

    if (is_shredded_variant() && other->is_shredded_variant()) {
        bool prepared = prepare_shredded_schema_for_append(*other);
        if (!prepared) {
            LOG(WARNING) << "failed to align shredded schema for append_value_multiple_times, fallback to row append";
            VariantRowValue row_buffer;
            const VariantRowValue* row = other->get_row_value(index, &row_buffer);
            for (uint32_t i = 0; i < count; ++i) {
                append(row);
            }
            return;
        }
        DCHECK(is_equal_schema(other) || _typed_columns.size() >= other->_typed_columns.size());
        DCHECK_EQ(_metadata_column != nullptr, other->_metadata_column != nullptr)
                << "metadata column presence mismatch for shredded append_value_multiple_times";
        DCHECK_EQ(_remain_value_column != nullptr, other->_remain_value_column != nullptr)
                << "remain column presence mismatch for shredded append_value_multiple_times";
        if (_metadata_column != nullptr) {
            _metadata_column->append_value_multiple_times(*other->_metadata_column, index, count);
        }
        if (_remain_value_column != nullptr) {
            _remain_value_column->append_value_multiple_times(*other->_remain_value_column, index, count);
        }

        for (size_t i = 0; i < _typed_columns.size(); ++i) {
            if (i < other->_typed_columns.size()) {
                _typed_columns[i]->append_value_multiple_times(*other->_typed_columns[i], index, count);
            } else {
                _typed_columns[i]->append_nulls(count);
            }
        }
        DCHECK(_is_shredded_row_aligned());
        return;
    }

    if (other->is_shredded_variant()) {
        // Destination was row mode and has just been initialized into shredded mode.
        bool prepared = prepare_shredded_schema_for_append(*other);
        DCHECK(prepared);
        if (!prepared) {
            LOG(WARNING) << "failed to align shredded schema for append_value_multiple_times, fallback to row append";
            VariantRowValue row_buffer;
            const VariantRowValue* row = other->get_row_value(index, &row_buffer);
            for (uint32_t i = 0; i < count; ++i) {
                append(row);
            }
            return;
        }
        DCHECK(is_equal_schema(other) || _typed_columns.size() >= other->_typed_columns.size());
        DCHECK_EQ(_metadata_column != nullptr, other->_metadata_column != nullptr)
                << "metadata column presence mismatch for shredded append_value_multiple_times";
        DCHECK_EQ(_remain_value_column != nullptr, other->_remain_value_column != nullptr)
                << "remain column presence mismatch for shredded append_value_multiple_times";
        if (_metadata_column != nullptr) {
            _metadata_column->append_value_multiple_times(*other->_metadata_column, index, count);
        }
        if (_remain_value_column != nullptr) {
            _remain_value_column->append_value_multiple_times(*other->_remain_value_column, index, count);
        }
        for (size_t i = 0; i < _typed_columns.size(); ++i) {
            if (i < other->_typed_columns.size()) {
                _typed_columns[i]->append_value_multiple_times(*other->_typed_columns[i], index, count);
            } else {
                _typed_columns[i]->append_nulls(count);
            }
        }
        DCHECK(_is_shredded_row_aligned());
        return;
    }

    DCHECK(is_shredded_variant());
    auto* row = other->get_object(index);
    for (uint32_t i = 0; i < count; ++i) {
        append(row);
    }
}

void VariantColumn::append_shredded(Slice metadata, Slice remain_value) {
    DCHECK(is_shredded_variant());
    DCHECK(_metadata_column != nullptr);
    DCHECK(_remain_value_column != nullptr);
    _metadata_column->append_datum(Datum(metadata));
    _remain_value_column->append_datum(Datum(remain_value));
    for (auto& col : _typed_columns) {
        col->append_nulls(1);
    }
}

void VariantColumn::append(const VariantRowValue& object) {
    append(&object);
}

void VariantColumn::append_shredded_null() {
    DCHECK(is_shredded_variant());
    append_nulls(1);
}

void VariantColumn::append(const VariantRowValue* object) {
    if (is_shredded_variant()) {
        if (object == nullptr) {
            append_nulls(1);
        } else {
            if (!has_metadata_column() || !has_remain_value()) {
                if (!_promote_typed_only_to_base_shredded()) {
                    LOG(WARNING) << "failed to promote typed-only variant before append";
                    append_nulls(1);
                    return;
                }
            }
            auto metadata_raw = object->get_metadata().raw();
            auto value_raw = object->get_value().raw();
            append_shredded(Slice(metadata_raw.data(), metadata_raw.size()), Slice(value_raw.data(), value_raw.size()));
        }
        return;
    }
    BaseClass::append(object);
}

bool VariantColumn::append_nulls(size_t count) {
    if (is_shredded_variant()) {
        if (_metadata_column != nullptr) {
            _metadata_column->append_nulls(count);
        }
        if (_remain_value_column != nullptr) {
            _remain_value_column->append_nulls(count);
        }
        for (auto& col : _typed_columns) {
            col->append_nulls(count);
        }
        return true;
    }
    for (size_t i = 0; i < count; ++i) {
        VariantRowValue null_row = VariantRowValue::from_null();
        append(&null_row);
    }
    return true;
}

void VariantColumn::append_default() {
    append_nulls(1);
}

void VariantColumn::append_default(size_t count) {
    append_nulls(count);
}

size_t VariantColumn::size() const {
    if (is_shredded_variant()) {
        return _shredded_num_rows();
    }
    return BaseClass::size();
}

size_t VariantColumn::capacity() const {
    if (!is_shredded_variant()) {
        return BaseClass::capacity();
    }
    size_t cap = 0;
    if (_metadata_column != nullptr) {
        cap += _metadata_column->capacity();
    }
    if (_remain_value_column != nullptr) {
        cap += _remain_value_column->capacity();
    }
    for (const auto& col : _typed_columns) {
        cap += col->capacity();
    }
    return cap;
}

size_t VariantColumn::byte_size(size_t from, size_t sz) const {
    if (!is_shredded_variant()) {
        return BaseClass::byte_size(from, sz);
    }
    size_t bytes = 0;
    if (_metadata_column != nullptr) {
        bytes += _metadata_column->byte_size(from, sz);
    }
    if (_remain_value_column != nullptr) {
        bytes += _remain_value_column->byte_size(from, sz);
    }
    for (const auto& col : _typed_columns) {
        bytes += col->byte_size(from, sz);
    }
    return bytes;
}

void VariantColumn::resize(size_t n) {
    if (!is_shredded_variant()) {
        BaseClass::resize(n);
        return;
    }
    if (_metadata_column != nullptr) {
        _metadata_column->resize(n);
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->resize(n);
    }
    for (auto& col : _typed_columns) {
        col->resize(n);
    }
}

void VariantColumn::assign(size_t n, size_t idx) {
    if (!is_shredded_variant()) {
        BaseClass::assign(n, idx);
        return;
    }
    if (_metadata_column != nullptr) {
        _metadata_column->assign(n, idx);
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->assign(n, idx);
    }
    for (auto& col : _typed_columns) {
        col->assign(n, idx);
    }
}

size_t VariantColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    if (!is_shredded_variant()) {
        return BaseClass::filter_range(filter, from, to);
    }
    size_t result_size = to - from;
    bool initialized = false;
    auto apply_filter = [&](MutableColumnPtr& col) {
        if (col == nullptr) {
            return;
        }
        size_t s = col->filter_range(filter, from, to);
        if (!initialized) {
            result_size = s;
            initialized = true;
        } else {
            DCHECK_EQ(result_size, s);
        }
    };
    apply_filter(_metadata_column);
    apply_filter(_remain_value_column);
    for (auto& col : _typed_columns) {
        apply_filter(col);
    }
    return result_size;
}

void VariantColumn::swap_column(Column& rhs) {
    auto& other = down_cast<VariantColumn&>(rhs);
    BaseClass::swap_column(other);
    std::swap(_shredded_paths, other._shredded_paths);
    std::swap(_shredded_types, other._shredded_types);
    std::swap(_typed_columns, other._typed_columns);
    std::swap(_metadata_column, other._metadata_column);
    std::swap(_remain_value_column, other._remain_value_column);
}

void VariantColumn::reset_column() {
    BaseClass::reset_column();
    clear_shredded_columns();
}

void VariantColumn::check_or_die() const {
    if (!is_shredded_variant()) {
        BaseClass::check_or_die();
        return;
    }
    DCHECK(_is_shredded_schema_valid());
    DCHECK(_is_shredded_row_aligned());
    if (_metadata_column != nullptr) {
        _metadata_column->check_or_die();
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->check_or_die();
    }
    for (const auto& col : _typed_columns) {
        col->check_or_die();
    }
}

bool VariantColumn::is_shredded_variant() const {
    return !_typed_columns.empty() || (_metadata_column != nullptr && _remain_value_column != nullptr);
}

bool VariantColumn::is_typed_only_variant() const {
    return !_typed_columns.empty() && !has_metadata_column() && !has_remain_value();
}

bool VariantColumn::is_root_typed_only_variant() const {
    return is_typed_only_variant() && _shredded_paths.size() == 1 && _shredded_paths[0].empty() &&
           _typed_columns.size() == 1;
}

const VariantRowValue* VariantColumn::get_row_value(size_t idx, VariantRowValue* output) const {
    if (is_shredded_variant()) {
        if (output == nullptr || !try_materialize_row(idx, output)) {
            return nullptr;
        }
        return output;
    }
    return get_object(idx);
}

static bool get_binary_slice(const Column* column, size_t idx, Slice* slice) {
    if (column == nullptr || slice == nullptr) {
        return false;
    }
    if (column->is_constant()) {
        const auto* const_column = down_cast<const ConstColumn*>(column);
        return get_binary_slice(const_column->data_column().get(), 0, slice);
    }
    if (column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(column);
        if (nullable->is_null(idx)) {
            return false;
        }
        return get_binary_slice(nullable->data_column().get(), idx, slice);
    }
    if (!column->is_binary()) {
        return false;
    }
    const auto* data = down_cast<const BinaryColumn*>(column);
    *slice = data->get_slice(idx);
    return true;
}

bool VariantColumn::try_materialize_row(size_t idx, VariantRowValue* output) const {
    if (output == nullptr || !is_shredded_variant()) {
        return false;
    }

    if (has_metadata_column() && has_remain_value()) {
        Slice metadata_slice;
        Slice remain_slice;
        if (!get_binary_slice(_metadata_column.get(), idx, &metadata_slice) ||
            !get_binary_slice(_remain_value_column.get(), idx, &remain_slice)) {
            return false;
        }
        const std::string_view metadata_raw(metadata_slice.data, metadata_slice.size);
        const std::string_view remain_raw(remain_slice.data, remain_slice.size);
        if (rebuild_row_from_base_shredded(this, idx, output, metadata_raw, remain_raw)) {
            return true;
        }
        // Best-effort fallback: preserve base shredded payload even if typed overlay reconstruction fails.
        *output = VariantRowValue(metadata_raw, remain_raw);
        return true;
    }

    if (!is_typed_only_variant()) {
        return false;
    }
    return rebuild_row_from_typed_columns(this, idx, output);
}

void VariantColumn::set_shredded_columns(std::vector<std::string> paths, std::vector<TypeDescriptor> type_descs,
                                         MutableColumns columns, MutableColumnPtr metadata_column,
                                         MutableColumnPtr remain_value_column) {
    _shredded_paths = std::move(paths);
    _shredded_types = std::move(type_descs);
    _typed_columns = std::move(columns);
    _metadata_column = std::move(metadata_column);
    _remain_value_column = std::move(remain_value_column);

    DCHECK(_is_shredded_schema_valid()) << "Invalid shredded schema in VariantColumn";
    DCHECK(_is_shredded_row_aligned()) << "Invalid shredded row alignment in VariantColumn";
}

void VariantColumn::clear_shredded_columns() {
    _shredded_paths.clear();
    _shredded_types.clear();
    _typed_columns.clear();
    _metadata_column.reset();
    _remain_value_column.reset();
}

Status VariantColumn::init_shredded_schema_from(const VariantColumn& other) {
    if (!other.is_shredded_variant()) {
        return Status::InvalidArgument("source variant is not shredded");
    }
    if (is_shredded_variant()) {
        return Status::InvalidArgument("destination variant is already shredded");
    }
    if (size() != 0) {
        return Status::InvalidArgument("destination variant must be empty when initializing shredded schema");
    }
    _init_container_schema_from(other);
    return Status::OK();
}

bool VariantColumn::prepare_shredded_schema_for_append(const VariantColumn& src) {
    if (!src.is_shredded_variant()) {
        return true;
    }
    if (!is_shredded_variant()) {
        _init_container_schema_from(src);
        return true;
    }
    if (is_equal_schema(&src)) {
        return true;
    }
    return align_shredded_schema_with(src);
}

void VariantColumn::_init_container_schema_from(const VariantColumn& other) {
    DCHECK(other.is_shredded_variant());
    BaseClass::reset_column();
    _shredded_paths = other._shredded_paths;
    _shredded_types = other._shredded_types;

    _typed_columns.clear();
    _typed_columns.reserve(other._typed_columns.size());
    for (const auto& col : other._typed_columns) {
        _typed_columns.emplace_back(col->clone_empty());
    }

    _metadata_column = other._metadata_column != nullptr ? other._metadata_column->clone_empty() : nullptr;
    _remain_value_column = other._remain_value_column != nullptr ? other._remain_value_column->clone_empty() : nullptr;
}

void VariantColumn::_append_container_rows(const VariantColumn& src, size_t offset, size_t count) {
    DCHECK(is_shredded_variant());
    DCHECK(src.is_shredded_variant());
    DCHECK_GE(_typed_columns.size(), src._typed_columns.size());
    DCHECK_EQ(_metadata_column != nullptr, src._metadata_column != nullptr)
            << "metadata column presence mismatch for shredded append";
    DCHECK_EQ(_remain_value_column != nullptr, src._remain_value_column != nullptr)
            << "remain column presence mismatch for shredded append";

    if (_metadata_column != nullptr) {
        _metadata_column->append(*src._metadata_column, offset, count);
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->append(*src._remain_value_column, offset, count);
    }
    for (size_t i = 0; i < _typed_columns.size(); ++i) {
        if (i < src._typed_columns.size()) {
            _typed_columns[i]->append(*src._typed_columns[i], offset, count);
        } else {
            _typed_columns[i]->append_nulls(count);
        }
    }

    DCHECK(_is_shredded_row_aligned());
}

void VariantColumn::_append_container_rows_selective(const VariantColumn& src, const uint32_t* indexes, uint32_t from,
                                                     uint32_t count) {
    DCHECK(is_shredded_variant());
    DCHECK(src.is_shredded_variant());
    DCHECK_GE(_typed_columns.size(), src._typed_columns.size());
    DCHECK_EQ(_metadata_column != nullptr, src._metadata_column != nullptr)
            << "metadata column presence mismatch for shredded append_selective";
    DCHECK_EQ(_remain_value_column != nullptr, src._remain_value_column != nullptr)
            << "remain column presence mismatch for shredded append_selective";

    if (_metadata_column != nullptr) {
        _metadata_column->append_selective(*src._metadata_column, indexes, from, count);
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->append_selective(*src._remain_value_column, indexes, from, count);
    }
    for (size_t i = 0; i < _typed_columns.size(); ++i) {
        if (i < src._typed_columns.size()) {
            _typed_columns[i]->append_selective(*src._typed_columns[i], indexes, from, count);
        } else {
            _typed_columns[i]->append_nulls(count);
        }
    }

    DCHECK(_is_shredded_row_aligned());
}

bool VariantColumn::_is_shredded_schema_valid() const {
    if (_shredded_paths.size() != _shredded_types.size()) {
        return false;
    }
    if (_shredded_paths.size() != _typed_columns.size()) {
        return false;
    }
    for (const auto& column : _typed_columns) {
        if (column == nullptr) {
            return false;
        }
    }

    if (has_metadata_column() != has_remain_value()) {
        return false;
    }

    bool has_root_path = false;
    bool has_non_root_path = false;
    std::unordered_set<std::string_view> unique_paths;
    unique_paths.reserve(_shredded_paths.size());
    for (const auto& path : _shredded_paths) {
        if (!unique_paths.emplace(path).second) {
            return false;
        }
        if (path.empty()) {
            has_root_path = true;
        } else {
            has_non_root_path = true;
        }
    }
    if (has_root_path && (has_non_root_path || _shredded_paths.size() != 1)) {
        return false;
    }

    if (is_typed_only_variant()) {
        return true;
    }
    return true;
}

size_t VariantColumn::_shredded_num_rows() const {
    if (_metadata_column != nullptr) {
        return _metadata_column->size();
    }
    if (_remain_value_column != nullptr) {
        return _remain_value_column->size();
    }
    if (!_typed_columns.empty()) {
        return _typed_columns[0]->size();
    }
    return 0;
}

bool VariantColumn::_promote_typed_only_to_base_shredded() {
    if (!is_typed_only_variant()) {
        return true;
    }

    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    size_t rows = _shredded_num_rows();
    metadata->reserve(rows);
    remain->reserve(rows);

    // Promote typed-only to base_shredded with an empty base row per record.
    // This keeps single ownership semantics: typed paths stay in typed_columns;
    // remain stores no duplicated typed payload during promotion.
    VariantRowValue null_base = VariantRowValue::from_null();
    std::string_view metadata_raw = null_base.get_metadata().raw();
    std::string_view value_raw = null_base.get_value().raw();
    for (size_t i = 0; i < rows; ++i) {
        metadata->append(Slice(metadata_raw.data(), metadata_raw.size()));
        remain->append(Slice(value_raw.data(), value_raw.size()));
    }

    _metadata_column = std::move(metadata);
    _remain_value_column = std::move(remain);
    return _is_shredded_schema_valid() && _is_shredded_row_aligned();
}

bool VariantColumn::_is_shredded_row_aligned() const {
    size_t rows = _shredded_num_rows();
    if (_metadata_column != nullptr && _metadata_column->size() != rows) {
        return false;
    }
    if (_remain_value_column != nullptr && _remain_value_column->size() != rows) {
        return false;
    }
    for (const auto& column : _typed_columns) {
        if (column->size() != rows) {
            return false;
        }
    }
    return true;
}

bool VariantColumn::is_equal_schema(const VariantColumn* other) const {
    if (other == nullptr) {
        return false;
    }
    if (is_shredded_variant() != other->is_shredded_variant()) {
        return false;
    }
    if (!is_shredded_variant()) {
        return true;
    }
    if (_shredded_paths != other->_shredded_paths) {
        return false;
    }
    if (_shredded_types != other->_shredded_types) {
        return false;
    }
    if (has_metadata_column() != other->has_metadata_column()) {
        return false;
    }
    if (has_remain_value() != other->has_remain_value()) {
        return false;
    }
    return _typed_columns.size() == other->_typed_columns.size();
}

bool VariantColumn::align_shredded_schema_with(const VariantColumn& src) {
    if (!is_shredded_variant() || !src.is_shredded_variant()) {
        LOG(WARNING) << "align_shredded_schema_with requires both columns in shredded mode";
        return false;
    }
    if (!_is_shredded_schema_valid() || !_is_shredded_row_aligned()) {
        LOG(WARNING) << "align_shredded_schema_with dst invariant check failed"
                     << ", schema_valid=" << _is_shredded_schema_valid()
                     << ", row_aligned=" << _is_shredded_row_aligned();
        return false;
    }
    if (!src._is_shredded_schema_valid() || !src._is_shredded_row_aligned()) {
        LOG(WARNING) << "align_shredded_schema_with src invariant check failed"
                     << ", schema_valid=" << src._is_shredded_schema_valid()
                     << ", row_aligned=" << src._is_shredded_row_aligned();
        return false;
    }
    if (has_metadata_column() != src.has_metadata_column()) {
        LOG(WARNING) << "align_shredded_schema_with metadata presence mismatch"
                     << ", dst_has_metadata=" << has_metadata_column()
                     << ", src_has_metadata=" << src.has_metadata_column();
        return false;
    }
    if (has_remain_value() != src.has_remain_value()) {
        LOG(WARNING) << "align_shredded_schema_with remain presence mismatch"
                     << ", dst_has_remain=" << has_remain_value() << ", src_has_remain=" << src.has_remain_value();
        return false;
    }

    if (is_equal_schema(&src)) {
        return true;
    }

    std::unordered_map<std::string_view, size_t> src_index_by_path;
    src_index_by_path.reserve(src._shredded_paths.size());
    for (size_t i = 0; i < src._shredded_paths.size(); ++i) {
        if (!src_index_by_path.emplace(src._shredded_paths[i], i).second) {
            LOG(WARNING) << "align_shredded_schema_with duplicate path in src: " << src._shredded_paths[i];
            return false;
        }
    }

    std::unordered_map<std::string_view, size_t> dst_index_by_path;
    dst_index_by_path.reserve(_shredded_paths.size());
    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        if (!dst_index_by_path.emplace(_shredded_paths[i], i).second) {
            LOG(WARNING) << "align_shredded_schema_with duplicate path in dst: " << _shredded_paths[i];
            return false;
        }
    }

    // Validate type compatibility for overlapping paths.
    for (size_t i = 0; i < src._shredded_paths.size(); ++i) {
        const auto& path = src._shredded_paths[i];
        auto dst_it = dst_index_by_path.find(path);
        if (dst_it == dst_index_by_path.end()) {
            continue;
        }
        if (_shredded_types[dst_it->second] != src._shredded_types[i]) {
            LOG(WARNING) << "align_shredded_schema_with type conflict on path=" << path
                         << ", dst_type=" << _shredded_types[dst_it->second].debug_string()
                         << ", src_type=" << src._shredded_types[i].debug_string();
            return false;
        }
    }

    std::vector<std::string> target_paths;
    std::vector<TypeDescriptor> target_types;
    target_paths.reserve(src._shredded_paths.size() + _shredded_paths.size());
    target_types.reserve(src._shredded_types.size() + _shredded_types.size());

    // Canonical order for append fast path:
    // 1) source paths in source order; 2) destination-only paths in destination order.
    for (size_t i = 0; i < src._shredded_paths.size(); ++i) {
        target_paths.emplace_back(src._shredded_paths[i]);
        target_types.emplace_back(src._shredded_types[i]);
    }
    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        if (src_index_by_path.find(_shredded_paths[i]) != src_index_by_path.end()) {
            continue;
        }
        target_paths.emplace_back(_shredded_paths[i]);
        target_types.emplace_back(_shredded_types[i]);
    }

    size_t dst_rows = _shredded_num_rows();
    std::vector<std::string> old_paths = std::move(_shredded_paths);
    MutableColumns old_typed_columns = std::move(_typed_columns);

    std::unordered_map<std::string_view, size_t> old_index_by_path;
    old_index_by_path.reserve(old_paths.size());
    for (size_t i = 0; i < old_paths.size(); ++i) {
        old_index_by_path.emplace(old_paths[i], i);
    }

    _shredded_paths = std::move(target_paths);
    _shredded_types = std::move(target_types);
    _typed_columns.clear();
    _typed_columns.reserve(_shredded_paths.size());

    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        auto old_it = old_index_by_path.find(_shredded_paths[i]);
        if (old_it != old_index_by_path.end()) {
            _typed_columns.emplace_back(std::move(old_typed_columns[old_it->second]));
            continue;
        }

        auto new_column = ColumnHelper::create_column(_shredded_types[i], true);
        if (dst_rows > 0) {
            new_column->append_nulls(dst_rows);
        }
        _typed_columns.emplace_back(std::move(new_column));
    }

    return _is_shredded_schema_valid() && _is_shredded_row_aligned();
}

std::string VariantColumn::debug_item(size_t idx) const {
    VariantRowValue row;
    const VariantRowValue* value = get_row_value(idx, &row);
    if (value == nullptr) {
        return "";
    }
    // For debug display, use UTC timezone to show timestamps consistently
    auto json_result = value->to_json(cctz::utc_time_zone());
    if (!json_result.ok()) {
        return "";
    }
    return json_result.value();
}

std::string VariantColumn::debug_string() const {
    std::string result = "[";
    for (size_t i = 0; i < size(); ++i) {
        if (i > 0) {
            result += ", ";
        }
        result += debug_item(i);
    }
    result += "]";
    return result;
}

} // namespace starrocks
