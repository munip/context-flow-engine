#pragma once

#include "common.h"
#include "memory_pool.h"
#include <vector>
#include <map>
#include <atomic>
#include <memory>

namespace nscfstore {

struct SSTableHeader {
    uint64_t magic;
    uint32_t version;
    uint64_t created_time;
    uint64_t entry_count;
    uint64_t data_size;
    uint32_t block_size;
    uint32_t compression_type;
    uint64_t index_offset;
    uint64_t index_size;
    uint64_t filter_offset;
    uint64_t filter_size;
};

struct DataBlock {
    std::vector<char> data;
    std::vector<uint32_t> restart_points;
    uint32_t num_restarts;
};

struct IndexEntry {
    Key first_key;
    uint64_t offset;
    uint64_t size;
    uint64_t sequence_number;
};

class BloomFilter {
public:
    explicit BloomFilter(size_t expected_items, double false_positive_rate = 0.01);
    
    void add(const Key& key);
    bool might_contain(const Key& key) const;
    
    std::vector<char> serialize() const;
    void deserialize(const std::vector<char>& data);
    
private:
    std::vector<uint64_t> bits_;
    size_t num_hash_functions_;
    
    std::vector<uint32_t> hash_keys(const Key& key) const;
};

class SSTableBuilder {
public:
    explicit SSTableBuilder(const std::string& file_path, 
                           uint32_t block_size = 4096,
                           uint32_t compression_type = 0);
    ~SSTableBuilder();
    
    bool add(const Key& key, const Value& value, uint64_t sequence);
    bool finish();
    
private:
    std::string file_path_;
    int fd_;
    uint32_t block_size_;
    uint32_t compression_type_;
    
    // Current block being built
    std::vector<std::pair<Key, Value>> current_block_;
    std::vector<IndexEntry> index_entries_;
    std::unique_ptr<BloomFilter> filter_;
    
    uint64_t current_offset_;
    uint64_t entry_count_;
    
    bool write_current_block();
    bool write_index();
    bool write_filter();
    bool write_header();
    
    std::vector<char> compress_block(const std::vector<char>& data);
};

class SSTableReader {
public:
    explicit SSTableReader(const std::string& file_path);
    ~SSTableReader();
    
    bool open();
    bool find(const Key& key, Value& value);
    bool might_contain(const Key& key);
    
    // Iterator for range scans
    class Iterator {
    public:
        Iterator(SSTableReader* reader, const Key& start_key = "");
        bool next();
        const Key& key() const;
        const Value& value() const;
        bool is_valid() const;
        
    private:
        SSTableReader* reader_;
        Key current_key_;
        Value current_value_;
        size_t current_block_index_;
        size_t current_entry_index_;
        DataBlock current_block_;
        bool valid_;
        
        bool load_block(size_t block_index);
        bool find_in_block(const Key& key);
    };
    
    Iterator begin(const Key& start_key = "") { return Iterator(this, start_key); }
    
    const SSTableHeader& header() const { return header_; }
    
private:
    std::string file_path_;
    int fd_;
    SSTableHeader header_;
    
    std::vector<IndexEntry> index_;
    std::unique_ptr<BloomFilter> filter_;
    
    bool read_header();
    bool read_index();
    bool read_filter();
    bool read_block(uint64_t offset, uint64_t size, DataBlock& block);
    
    std::vector<char> decompress_block(const std::vector<char>& data);
};

class SSTableManager {
public:
    explicit SSTableManager(uint32_t shard_id, const std::string& data_dir, 
                           ShardMemoryPool* pool);
    ~SSTableManager();
    
    // SSTable operations
    bool create_sstable(const std::vector<std::pair<Key, Value>>& entries,
                       const std::vector<uint64_t>& sequences);
    
    bool get(const Key& key, Value& value);
    std::vector<std::pair<Key, Value>> scan(const Key& start_key, const Key& end_key);
    
    // Compaction
    bool compact();
    bool compact_level(uint32_t level);
    
    // Statistics
    struct Stats {
        uint64_t total_sstables;
        uint64_t total_entries;
        uint64_t total_size;
        uint32_t max_level;
    };
    
    Stats get_stats() const;
    
private:
    uint32_t shard_id_;
    std::string data_dir_;
    ShardMemoryPool* memory_pool_;
    
    // Multi-level SSTable organization
    std::vector<std::vector<std::unique_ptr<SSTableReader>>> levels_;
    std::mutex levels_mutex_;
    
    // Compaction state
    std::atomic<bool> compaction_running_{false};
    std::thread compaction_thread_;
    
    void scan_data_directory();
    std::string get_sstable_path(uint32_t level, uint64_t id);
    bool load_sstable(uint32_t level, const std::string& file_path);
    
    // Compaction helpers
    std::vector<std::pair<Key, Value>> merge_sstables(
        const std::vector<SSTableReader*>& sstables);
    bool select_compaction_candidates(uint32_t level, 
                                    std::vector<SSTableReader*>& candidates);
};

} // namespace nscfstore
