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

#include <memory>
#include <tuple>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/persistent_index.pb.h"
#include "storage/edit_version.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "util/bloom_filter.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks {

class Tablet;
class Schema;
class Column;
class PrimaryKeyDump;

class TabletLoader {
public:
    virtual ~TabletLoader() = default;
    virtual starrocks::Schema generate_pkey_schema() = 0;
    virtual DataDir* data_dir() = 0;
    virtual TTabletId tablet_id() = 0;
    // return latest applied (publish in cloud native) version
    virtual StatusOr<EditVersion> applied_version() = 0;
    // Do some special setting if need
    virtual void setting() = 0;
    // iterator all rowset and get their iterator and basic stat
    virtual Status rowset_iterator(
            const Schema& pkey_schema,
            const std::function<Status(const std::vector<ChunkIteratorPtr>&, uint32_t)>& handler) = 0;

    virtual void set_write_amp_score(double score) = 0;

    size_t total_data_size() const { return _total_data_size; }
    size_t total_segments() const { return _total_segments; }
    size_t rowset_num() const { return _rowset_num; };

protected:
    size_t _total_data_size = 0;
    size_t _total_segments = 0;
    size_t _rowset_num = 0;
};

namespace lake {
class LakeLocalPersistentIndex;
}

// Add version for persistent index file to support future upgrade compatibility
// There is only one version for now
enum PersistentIndexFileVersion {
    PERSISTENT_INDEX_VERSION_UNKNOWN = 0,
    PERSISTENT_INDEX_VERSION_1,
    PERSISTENT_INDEX_VERSION_2,
    PERSISTENT_INDEX_VERSION_3,
    PERSISTENT_INDEX_VERSION_4,
    PERSISTENT_INDEX_VERSION_5,
    PERSISTENT_INDEX_VERSION_6,
    PERSISTENT_INDEX_VERSION_7
};

static constexpr uint64_t NullIndexValue = -1;
static std::string MergeSuffix = ".merged";
static std::string BloomFilterSuffix = ".bf";

extern bool write_pindex_bf;

enum CommitType {
    kFlush = 0,
    kSnapshot = 1,
    kAppendWAL = 2,
};

struct IOStat {
    uint32_t read_iops = 0;
    uint32_t filtered_kv_cnt = 0;
    uint64_t get_in_shard_cost = 0;
    uint64_t read_io_bytes = 0;
    uint64_t l0_write_cost = 0;
    uint64_t l1_l2_read_cost = 0;
    uint64_t flush_or_wal_cost = 0;
    uint64_t compaction_cost = 0;
    uint64_t reload_meta_cost = 0;

    std::string print_str() {
        return fmt::format(
                "IOStat read_iops: {} filtered_kv_cnt: {} get_in_shard_cost: {} read_io_bytes: {} "
                "l0_write_cost: {} "
                "l1_l2_read_cost: {} flush_or_wal_cost: {} compaction_cost: {} reload_meta_cost: {}",
                read_iops, filtered_kv_cnt, get_in_shard_cost, read_io_bytes, l0_write_cost, l1_l2_read_cost,
                flush_or_wal_cost, compaction_cost, reload_meta_cost);
    }
};

// Use `uint8_t[8]` to store the value of a `uint64_t` to reduce memory cost in phmap
struct IndexValue {
    uint8_t v[8];
    IndexValue() = default;
    explicit IndexValue(const uint64_t val) { UNALIGNED_STORE64(v, val); }

    uint64_t get_value() const { return UNALIGNED_LOAD64(v); }
    uint32_t get_rssid() const { return (uint32_t)(get_value() >> 32); }
    uint32_t get_rowid() const { return (uint32_t)(get_value() & 0xFFFFFFFF); }
    bool operator==(const IndexValue& rhs) const { return memcmp(v, rhs.v, 8) == 0; }
    void operator=(uint64_t rhs) { return UNALIGNED_STORE64(v, rhs); }
};

using IndexValueWithVer = std::pair<int64_t, IndexValue>;

static constexpr size_t kIndexValueSize = 8;
static_assert(sizeof(IndexValue) == kIndexValueSize);
constexpr static size_t kSliceMaxFixLength = 64;

uint64_t key_index_hash(const void* data, size_t len);

using KeyInfo = std::pair<uint32_t, uint64_t>;
struct KeysInfo {
    std::vector<KeyInfo> key_infos;
    size_t size() const { return key_infos.size(); }

    void set_difference(KeysInfo& input) {
        std::vector<std::pair<uint32_t, uint64_t>> infos;
        std::set_difference(key_infos.begin(), key_infos.end(), input.key_infos.begin(), input.key_infos.end(),
                            std::back_inserter(infos), [](auto& a, auto& b) { return a.first < b.first; });
        key_infos.swap(infos);
    }
};

struct KVRef {
    const uint8_t* kv_pos;
    uint64_t hash;
    uint16_t size;
    KVRef() = default;
    KVRef(const uint8_t* kv_pos, uint64_t hash, uint16_t size) : kv_pos(kv_pos), hash(hash), size(size) {}
};

struct EditVersionWithMerge {
    EditVersionWithMerge(const EditVersion& ver, bool m) : version(ver), merged(m) {}
    EditVersionWithMerge(int64_t major, int64_t minor, bool m) : version(major, minor), merged(m) {}
    bool operator<(const EditVersionWithMerge& rhs) {
        if (version == rhs.version) {
            // version with .merged is bigger
            if (!merged && rhs.merged) {
                return true;
            } else {
                return false;
            }
        } else {
            return version < rhs.version;
        }
    }
    EditVersion version;
    // if this version is generated by major merge compaction
    bool merged{false};
};

struct IndexPage;
struct LargeIndexPage;
struct ImmutableIndexShard;
class PersistentIndex;
class ImmutableIndexWriter;

class MutableIndex {
public:
    MutableIndex();
    virtual ~MutableIndex();

    // batch get
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    // |idxes|: the target indexes of keys
    virtual Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
                       const std::vector<size_t>& idxes) const = 0;

    // batch upsert and get old value
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    // |idxes|: the target indexes of keys
    virtual Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                          size_t* num_found, const std::vector<size_t>& idxes) = 0;

    // batch upsert
    // |keys|: key array as raw buffer
    // |values|: value array
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found(or already exist) to this argument
    // |idxes|: the target indexes of keys
    virtual Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                          const std::vector<size_t>& idxes) = 0;

    // batch insert
    // |keys|: key array as raw buffer
    // |values|: value array
    // |idxes|: the target indexes of keys
    virtual Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) = 0;

    // batch erase(delete)
    // |keys|: key array as raw buffer
    // |old_values|: return old values for updates, or set to NullValue if not exists
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    // |idxes|: the target indexes of keys
    virtual Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                         const std::vector<size_t>& idxes) = 0;

    // batch replace
    // |keys|: key array as raw buffer
    // |values|: new value array
    // |replace_idxes|: the idx array of the kv needed to be replaced
    virtual Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) = 0;

    virtual Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                              std::unique_ptr<WritableFile>& index_file, uint64_t* page_size, uint32_t* checksum) = 0;

    // load wals
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    virtual Status load_wals(size_t n, const Slice* keys, const IndexValue* values) = 0;

    // load snapshot
    virtual Status load_snapshot(phmap::BinaryInputArchive& ar) = 0;

    // load according meta
    virtual Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) = 0;

    // get dump total size of hashmaps of shards
    virtual size_t dump_bound() = 0;

    virtual Status dump(phmap::BinaryOutputArchive& ar) = 0;

    // get all key-values pair references by shard, the result will remain valid until next modification
    // |nshard|: number of shard
    // |num_entry|: number of entries expected, it should be:
    //                 the num of KV entries if with_null == true
    //                 the num of KV entries excluding nulls if with_null == false
    // |with_null|: whether to include null entries
    // [not thread-safe]
    virtual std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                                 bool with_null) const = 0;

    virtual Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard,
                                            size_t npage_hint, size_t page_size, size_t nbucket,
                                            bool with_null) const = 0;

    // get the number of entries in the index (including NullIndexValue)
    virtual size_t size() const = 0;

    virtual size_t usage() const = 0;

    virtual size_t capacity() = 0;

    virtual void reserve(size_t size) = 0;

    virtual void clear() = 0;

    virtual size_t memory_usage() = 0;

    virtual Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) = 0;

    virtual void set_mutable_index_format_version(uint32_t ver) = 0;

    virtual Status completeness_check(phmap::BinaryInputArchive& ar) = 0;

    static StatusOr<std::unique_ptr<MutableIndex>> create(size_t key_size);

    static std::tuple<size_t, size_t, size_t> estimate_nshard_and_npage(const size_t total_kv_pairs_usage,
                                                                        const size_t total_kv_num);

    static size_t estimate_nbucket(size_t key_size, size_t size, size_t nshard, size_t npage);
};

class ShardByLengthMutableIndex {
public:
    ShardByLengthMutableIndex() = default;

    ShardByLengthMutableIndex(const size_t key_size, const std::string& path) // NOLINT
            : _fixed_key_size(key_size), _path(path) {}

    ~ShardByLengthMutableIndex() {
        if (_index_file) {
            WARN_IF_ERROR(_index_file->close(), "Failed to close index file:" + _index_file->filename());
        }
    }

    Status init();

    uint64_t file_size() {
        if (_index_file != nullptr) {
            return _index_file->size();
        } else {
            return 0;
        }
    }

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    // |num_found|: add the number of keys found to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status get(size_t n, const Slice* keys, IndexValue* values, size_t* num_found,
               std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch upsert and get old value
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |num_found|: add the number of keys found to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values, size_t* num_found,
                  std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |num_found|: add the number of keys found(or already exist) to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, size_t* num_found,
                  std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch insert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |check_l1_key_sizes|: a set of key size need to be checked in l1.
    Status insert(size_t n, const Slice* keys, const IndexValue* values, std::set<size_t>& check_l1_key_sizes);

    // batch erase(delete)
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values for updates, or set to NullValue if not exists
    // |num_found|: add the number of keys found to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status erase(size_t n, const Slice* keys, IndexValue* old_values, size_t* num_found,
                 std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch replace
    // |keys|: key array as raw buffer
    // |values|: new value array
    // |idxes|: the idx array of the kv needed to be replaced
    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes);

    Status append_wal(size_t n, const Slice* keys, const IndexValue* values);
    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes);

    // load snapshot
    Status load_snapshot(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& dumped_shard_idxes);

    // load according meta
    Status load(const MutableIndexMetaPB& meta);

    size_t dump_bound();

    Status dump(phmap::BinaryOutputArchive& ar, std::set<uint32_t>& dumped_shard_idxes);

    Status commit(MutableIndexMetaPB* meta, const EditVersion& version, const CommitType& type);

    // get all key-values pair references by shard, the result will remain valid until next modification
    // |num_entry|: number of entries expected, it should be:
    //                 the num of KV entries if with_null == true
    //                 the num of KV entries excluding nulls if with_null == false
    // |with_null|: whether to include null entries
    std::vector<std::pair<uint32_t, std::vector<std::vector<KVRef>>>> get_kv_refs_by_shard(size_t num_entry,
                                                                                           bool with_null);

    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys, size_t idx_begin,
                                                         size_t idx_end);
    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys,
                                                         const std::vector<size_t>& idxes);

    Status flush_to_immutable_index(const std::string& dir, const EditVersion& version, bool write_tmp_l1,
                                    bool keep_delete);

    // get the number of entries in the index (including NullIndexValue)
    size_t size();

    size_t capacity();

    size_t memory_usage();

    void clear();

    Status create_index_file(std::string& path);

    static StatusOr<std::unique_ptr<ShardByLengthMutableIndex>> create(size_t key_size, const std::string& path);

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb);

    Status check_snapshot_file(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& idxes);

private:
    friend class PersistentIndex;
    friend class starrocks::lake::LakeLocalPersistentIndex;

    template <int N>
    void _init_loop_helper();

private:
    uint32_t _fixed_key_size = -1;
    uint64_t _offset = 0;
    uint64_t _page_size = 0;
    uint32_t _checksum = 0;
    std::string _path;
    std::unique_ptr<WritableFile> _index_file;
    std::shared_ptr<FileSystem> _fs;
    std::vector<std::unique_ptr<MutableIndex>> _shards;
    // TODO: confirm whether can be just one shard in a offset, which means shard size always be 1, it can simplify the manager of various shards.
    // <key size, <shard offset, shard size>>
    std::map<uint32_t, std::pair<uint32_t, uint32_t>> _shard_info_by_key_size;
};

class ImmutableIndex {
public:
    // batch get
    // |n|: size of key/value array
    // |keys|: key array as slice array
    // |not_found|: information of keys not found in upper level, which needs to be checked in this level
    // |values|: value array for return values
    // |num_found|: add the number of keys found in L1 to this argument
    // |key_size|: the key size of keys array
    // |stat|: used for collect statistic
    Status get(size_t n, const Slice* keys, KeysInfo& keys_info, IndexValue* values, KeysInfo* found_keys_info,
               size_t key_size, IOStat* stat = nullptr);

    // batch check key existence
    Status check_not_exist(size_t n, const Slice* keys, size_t key_size);

    // get Immutable index file size;
    uint64_t file_size() {
        if (_file != nullptr) {
            auto res = _file->get_size();
            DCHECK(res.ok()) << res.status(); // FIXME: no abort
            return *res;
        } else {
            return 0;
        }
    }

    void clear() {
        if (_file != nullptr) {
            _file.reset();
        }
    }

    void destroy() {
        if (_file != nullptr) {
            WARN_IF_ERROR(FileSystem::Default()->delete_file(_file->filename()),
                          "Failed to delete file" + _file->filename());
            _file.reset();
        }
    }

    size_t total_usage() {
        size_t usage = 0;
        for (const auto& shard : _shards) {
            usage += shard.data_size;
        }
        return usage;
    }

    // return total kv count of this immutable index
    size_t total_size() {
        size_t size = 0;
        for (const auto& shard : _shards) {
            size += shard.size;
        }
        return size;
    }

    size_t memory_usage() {
        size_t mem_usage = 0;
        for (auto& bf : _bf_vec) {
            if (bf != nullptr) {
                mem_usage += bf->size();
            }
        }
        return mem_usage;
    }

    std::string filename() const {
        if (_file != nullptr) {
            return _file->filename();
        } else {
            return "";
        }
    }

    EditVersion version() const { return _version; }

    bool has_bf() { return !_bf_vec.empty(); }

    static StatusOr<std::unique_ptr<ImmutableIndex>> load(std::unique_ptr<RandomAccessFile>&& index_rb,
                                                          bool load_bf_data);

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb);

private:
    friend class PersistentIndex;
    friend class starrocks::lake::LakeLocalPersistentIndex;
    friend class ImmutableIndexWriter;

    Status _get_fixlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                     uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_varlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                     uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const;

    // get all the kv refs of a single shard by `shard_idx`, and add them to `kvs_by_shard`, the shard number of
    // kvs_by_shard may be different from this object's own shard number
    // NOTE: used by PersistentIndex only
    Status _get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx, uint32_t shard_bits,
                              std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys, const std::vector<KeyInfo>& keys_info,
                                IndexValue* values, KeysInfo* found_keys_info,
                                std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys, std::vector<KeyInfo>& keys_info,
                                IndexValue* values, KeysInfo* found_keys_info,
                                std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _split_keys_info_by_page(size_t shard_idx, std::vector<KeyInfo>& keys_info,
                                    std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page) const;

    Status _get_in_fixlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                        KeysInfo* found_keys_info,
                                        std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                        std::map<size_t, LargeIndexPage>& pages) const;

    Status _get_in_varlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                        KeysInfo* found_keys_info,
                                        std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                        std::map<size_t, LargeIndexPage>& pages) const;

    Status _read_page(size_t shard_idx, size_t pageid, LargeIndexPage* page, IOStat* stat) const;

    Status _get_in_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                 KeysInfo* found_keys_info, std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                 IOStat* stat) const;

    Status _get_in_shard(size_t shard_idx, size_t n, const Slice* keys, std::vector<KeyInfo>& keys_info,
                         IndexValue* values, KeysInfo* found_keys_info, IOStat* stat) const;

    Status _check_not_exist_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _check_not_exist_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _check_not_exist_in_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info) const;

    bool _need_bloom_filter(size_t idx_begin, size_t idx_end, std::vector<KeysInfo>& keys_info_by_shard) const;

    Status _prepare_bloom_filter(size_t idx_begin, size_t idx_end) const;

    bool _filter(size_t shard_idx, std::vector<KeyInfo>& keys_info, std::vector<KeyInfo>* res) const;

    std::unique_ptr<RandomAccessFile> _file;
    EditVersion _version;
    size_t _size = 0;

    struct ShardInfo {
        uint64_t offset;
        uint64_t bytes;
        uint32_t npage;
        uint32_t size; // kv count
        uint32_t key_size;
        uint32_t value_size;
        uint32_t nbucket;
        uint64_t data_size;
        uint64_t uncompressed_size;
        uint64_t page_size;
        std::vector<int32_t> page_off;
    };

    std::vector<ShardInfo> _shards;
    std::map<size_t, std::pair<size_t, size_t>> _shard_info_by_length;
    mutable std::vector<std::unique_ptr<BloomFilter>> _bf_vec;
    std::vector<size_t> _bf_off;
    CompressionTypePB _compression_type;
};

class ImmutableIndexWriter {
public:
    ~ImmutableIndexWriter();

    Status init(const string& idx_file_path, const EditVersion& version, bool sync_on_close);

    // write_shard() must be called serially in the order of key_size and it is caller's duty to guarantee this.
    Status write_shard(size_t key_size, size_t npage_hint, size_t page_size, size_t nbucket,
                       const std::vector<KVRef>& kvs);

    Status write_bf();

    Status finish();

    // return total kv count of this immutable index
    size_t total_kv_size() { return _total_kv_size; }

    size_t file_size() { return _total_kv_bytes + _total_bf_bytes; }

    bool bf_flushed() { return _bf_flushed; }

    size_t total_kv_num() { return _total; }

    std::string index_file() { return _idx_file_path; }

private:
    EditVersion _version;
    string _idx_file_path_tmp;
    string _idx_file_path;
    string _bf_file_path;
    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<WritableFile> _idx_wb;
    std::unique_ptr<WritableFile> _bf_wb;
    std::vector<size_t> _shard_bf_size;
    std::vector<std::unique_ptr<BloomFilter>> _bf_vec;
    std::map<size_t, std::pair<size_t, size_t>> _shard_info_by_length;
    size_t _nshard = 0;
    size_t _cur_key_size = -1;
    size_t _cur_value_size = 0;
    size_t _total = 0;
    size_t _total_moved = 0;
    size_t _total_kv_size = 0;
    size_t _total_kv_bytes = 0;
    size_t _total_bf_bytes = 0;
    ImmutableIndexMetaPB _meta;
    bool _bf_flushed = false;
};

// A persistent primary index contains an in-memory L0 and an on-SSD/NVMe L1,
// this saves memory usage comparing to the orig all-in-memory implementation.
// This is a internal class and is intended to be used by PrimaryIndex internally.
//
// Currently primary index is only modified in TabletUpdates::apply process, it's
// typical use pattern in apply:
//   pi.prepare(version)
//   if (pi.upsert(upsert_keys, values, old_values))
//   pi.erase(delete_keys, old_values)
//   pi.commit()
//   pi.on_commited()
// If any error occurred between prepare and on_commited, abort should be called, the
// index maybe corrupted, currently for simplicity, the whole index is cleared and rebuilt.
class PersistentIndex {
public:
    // |path|: directory that contains index files
    PersistentIndex(std::string path);
    virtual ~PersistentIndex();

    bool loaded() const { return (bool)_l0; }

    std::string path() const { return _path; }

    size_t key_size() const { return _key_size; }

    size_t size() const { return _size; }
    size_t usage() const { return _usage; }
    virtual size_t memory_usage() const { return _memory_usage.load(); }

    EditVersion version() const { return _version; }

    // create new empty index
    Status create(size_t key_size, const EditVersion& version);

    // load required states from underlying file
    Status load(const PersistentIndexMetaPB& index_meta);

    // build PersistentIndex from pre-existing tablet data
    Status load_from_tablet(Tablet* tablet);

    // start modification with intended version
    // | version |: intended commit version
    // | n |: deprecated
    Status prepare(const EditVersion& version, size_t n);

    // abort modification
    Status abort();

    // commit modification
    Status commit(PersistentIndexMetaPB* index_meta, IOStat* stat = nullptr);

    // apply modification
    Status on_commited();

    // batch index operations

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    virtual Status get(size_t n, const Slice* keys, IndexValue* values);

    Status get_from_one_immutable_index(ImmutableIndex* immu_index, size_t n, const Slice* keys, IndexValue* values,
                                        std::map<size_t, KeysInfo>* keys_info_by_key_size, KeysInfo* found_keys_info);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |stat|: used for collect statistic
    virtual Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                          IOStat* stat = nullptr);

    // batch replace without return old values
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |replace_indexes|: The index of the |keys| array that need to replace.
    virtual Status replace(size_t n, const Slice* keys, const IndexValue* values,
                           const std::vector<uint32_t>& replace_indexes);

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |check_l1|: also check l1 for insertion consistency(key must not exist previously), may imply heavy IO costs
    Status insert(size_t n, const Slice* keys, const IndexValue* values, bool check_l1);

    // batch erase
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values if key exist, or set to NullValue if not
    virtual Status erase(size_t n, const Slice* keys, IndexValue* old_values);

    // TODO(qzc): maybe unused, remove it or refactor it with the methods in use by template after a period of time
    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |src_rssid|: rssid array
    // |failed|: return not match rowid
    [[maybe_unused]] Status try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                        const std::vector<uint32_t>& src_rssid, std::vector<uint32_t>* failed);

    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |max_src_rssid|: maximum of rssid array
    // |failed|: return not match rowid
    virtual Status try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                               std::vector<uint32_t>* failed);

    Status flush_advance();

    std::vector<int8_t> test_get_move_buckets(size_t target, const uint8_t* bucket_packs_in_page);

    Status test_flush_varlen_to_immutable_index(const std::string& dir, const EditVersion& version, size_t num_entry,
                                                const Slice* keys, const IndexValue* values);

    bool is_error() { return _error; }

    // just for unit test
    bool has_bf() { return _l1_vec.empty() ? false : _l1_vec[0]->has_bf(); }

    Status major_compaction(DataDir* data_dir, int64_t tablet_id, std::shared_timed_mutex* mutex);

    Status TEST_major_compaction(PersistentIndexMetaPB& index_meta);

    static double major_compaction_score(const PersistentIndexMetaPB& index_meta);

    // not thread safe, just for unit test
    size_t kv_num_in_immutable_index() {
        size_t res = 0;
        for (int i = 0; i < _l1_vec.size(); i++) {
            res += _l1_vec[i]->total_size();
        }
        for (int i = 0; i < _l2_vec.size(); i++) {
            res += _l2_vec[i]->total_size();
        }
        return res;
    }

    // not thread safe, just for unit test
    std::pair<int64_t, int64_t> kv_stat_in_estimate_stats() {
        std::pair<int64_t, int64_t> res;
        for (auto [_, stat] : _usage_and_size_by_key_length) {
            res.first += stat.first;
            res.second += stat.second;
        }
        return res;
    }

    void clear_kv_stat() { _usage_and_size_by_key_length.clear(); }

    Status reset(Tablet* tablet, EditVersion version, PersistentIndexMetaPB* index_meta);

    void reset_cancel_major_compaction();

    static Status modify_l2_versions(const std::vector<EditVersion>& input_l2_versions,
                                     const EditVersion& output_l2_version, PersistentIndexMetaPB& index_meta);

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexMultiLevelPB* dump_pb);

    void test_calc_memory_usage() { return _calc_memory_usage(); }

    void test_force_dump();

protected:
    Status _delete_expired_index_file(const EditVersion& l0_version, const EditVersion& l1_version,
                                      const EditVersionWithMerge& min_l2_version);

    uint64_t _l2_file_size() const;

    Status _load_by_loader(TabletLoader* loader);

private:
    size_t _dump_bound();

    void _set_error(bool error, const string& msg) {
        _error = error;
        _error_msg = msg;
    }
    // check _l0 should dump as snapshot or not
    bool _can_dump_directly();
    bool _need_flush_advance();
    bool _need_merge_advance();
    Status _flush_advance_or_append_wal(size_t n, const Slice* keys, const IndexValue* values,
                                        std::vector<size_t>* replace_idxes);
    Status _delete_major_compaction_tmp_index_file();
    Status _delete_tmp_index_file();

    Status _flush_l0();

    Status _merge_compaction_internal(ImmutableIndexWriter* writer, int l1_start_idx, int l1_end_idx,
                                      std::map<uint32_t, std::pair<int64_t, int64_t>>& usage_and_size_stat,
                                      bool keep_delete);
    Status _merge_compaction_advance();
    // merge l0 and l1 into new l1, then clear l0
    Status _merge_compaction();

    Status _load(const PersistentIndexMetaPB& index_meta, bool reload = false);
    Status _reload(const PersistentIndexMetaPB& index_meta);

    // commit index meta
    Status _build_commit(TabletLoader* loader, PersistentIndexMetaPB& index_meta);

    // insert rowset data into persistent index
    Status _insert_rowsets(TabletLoader* loader, const Schema& pkey_schema, MutableColumnPtr pk_column);

    Status _get_from_immutable_index(size_t n, const Slice* keys, IndexValue* values,
                                     std::map<size_t, KeysInfo>& keys_info_by_key_size, IOStat* stat);

    Status _get_from_immutable_index_parallel(size_t n, const Slice* keys, IndexValue* values,
                                              std::map<size_t, KeysInfo>& keys_info_by_key_size);

    Status _update_usage_and_size_by_key_length(std::vector<std::pair<int64_t, int64_t>>& add_usage_and_size);

    void _get_stat_from_immutable_index(ImmutableIndex* immu_index, uint32_t key_size, size_t& total_size,
                                        size_t& total_usage);

    Status _minor_compaction(PersistentIndexMetaPB* index_meta);

    uint64_t _l1_l2_file_size() const;

    void _get_l2_stat(const std::vector<std::unique_ptr<ImmutableIndex>>& l2_vec,
                      std::map<uint32_t, std::pair<int64_t, int64_t>>& usage_and_size_stat);

    StatusOr<EditVersion> _major_compaction_impl(const std::vector<EditVersion>& l2_versions,
                                                 const std::vector<std::unique_ptr<ImmutableIndex>>& l2_vec);

    bool _enable_minor_compaction();

    size_t _get_tmp_l1_count();

    bool _l0_is_full(int64_t l1_l2_size = 0);

    bool _need_rebuild_index(const PersistentIndexMetaPB& index_meta);

    Status _reload_usage_and_size_by_key_length(size_t l1_idx_start, size_t l1_idx_end, bool contain_l2);

    // Calculate total memory usage after index been modified.
    void _calc_memory_usage();

    size_t _get_encoded_fixed_size(const Schema& schema);

protected:
    // index storage directory
    std::string _path;
    size_t _key_size = 0;
    size_t _size = 0;
    size_t _usage = 0;
    EditVersion _version;
    // _l1_version is used to get l1 file name, update in on_committed
    EditVersion _l1_version;
    std::unique_ptr<ShardByLengthMutableIndex> _l0;
    bool _has_l1 = false;
    std::shared_ptr<FileSystem> _fs;
    bool _dump_snapshot = false;
    bool _flushed = false;
    // add all l1 into vector
    std::vector<std::unique_ptr<ImmutableIndex>> _l1_vec;
    // The usage and size is not exactly accurate after reload persistent index from disk becaues
    // we ignore the overlap kvs between l0 and l1. The general accuracy can already be used as a
    // reference to estimate nshard and npages We don't persist the overlap kvs info to reduce the
    // write cost of PersistentIndexMeta
    std::map<uint32_t, std::pair<int64_t, int64_t>> _usage_and_size_by_key_length;
    std::vector<int> _l1_merged_num;
    // l2 files's version
    std::vector<EditVersionWithMerge> _l2_versions;
    // all l2
    std::vector<std::unique_ptr<ImmutableIndex>> _l2_vec;

    bool _cancel_major_compaction = false;

private:
    bool _need_bloom_filter = false;

    mutable std::mutex _get_lock;
    std::condition_variable _get_task_finished;
    size_t _running_get_task = 0;
    std::atomic<bool> _error{false};
    std::string _error_msg;
    std::vector<KeysInfo> _found_keys_info;
    // save bloom filter of l1 after merge compaction in order to skip read bloom filter file
    // std::vector<std::unique_ptr<BloomFilter>> _bf_vec;
    // set if major compaction is running
    std::atomic<bool> _major_compaction_running{false};
    // Latest major compaction time. In second.
    int64_t _latest_compaction_time = 0;
    // Re-calculated when commit end
    std::atomic<size_t> _memory_usage{0};
};

} // namespace starrocks
