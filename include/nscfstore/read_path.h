#pragma once

#include "common.h"
#include "memory_pool.h"
#include <atomic>
#include <vector>
#include <unordered_map>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <xxhash.h>

namespace nscfstore {

// Forward declarations
class SSTableReader;
class TieredCompactionManager;

// Optimized Bloom Filter with XXHash
class OptimizedBloomFilter {
public:
    explicit OptimizedBloomFilter(size_t expected_items, double false_positive_rate = 0.01);
    ~OptimizedBloomFilter();
    
    void add(const Key& key);
    bool might_contain(const Key& key) const;
    
    // Serialization for SSTable storage
    std::vector<char> serialize() const;
    bool deserialize(const std::vector<char>& data);
    
    // Memory usage
    size_t memory_usage() const;
    size_t num_items() const { return num_items_.load(); }
    
private:
    std::vector<uint64_t> bits_;
    size_t num_hash_functions_;
    size_t num_bits_;
    std::atomic<size_t> num_items_{0};
    
    // XXHash-based hashing
    std::vector<uint64_t> hash_keys(const Key& key) const;
    void set_bit(size_t index);
    bool get_bit(size_t index) const;
    
    static constexpr size_t ALIGNMENT = 64; // Cache line alignment
};

// Global Row Cache for hot context nodes
class GlobalRowCache {
public:
    struct CacheEntry {
        Value value;
        std::atomic<uint64_t> access_time;
        std::atomic<uint32_t> access_count;
        std::atomic<uint32_t> size_bytes;
        std::atomic<bool> valid;
        
        CacheEntry() : access_time(0), access_count(0), size_bytes(0), valid(false) {}
    };
    
    explicit GlobalRowCache(size_t capacity_bytes = 1024 * 1024 * 1024); // 1GB default
    ~GlobalRowCache();
    
    bool get(const Key& key, Value& value);
    void put(const Key& key, const Value& value);
    void remove(const Key& key);
    
    // Cache statistics
    struct Stats {
        uint64_t hits;
        uint64_t misses;
        uint64_t evictions;
        double hit_rate;
        size_t current_usage;
        size_t capacity;
        uint32_t num_entries;
    };
    
    Stats get_stats() const;
    void reset_stats();
    
    // Cache management
    void clear();
    void resize(size_t new_capacity);
    
private:
    size_t capacity_bytes_;
    std::atomic<size_t> current_usage_{0};
    
    // Hash map for cache entries
    std::unordered_map<Key, std::unique_ptr<CacheEntry>> cache_;
    mutable std::shared_mutex cache_mutex_;
    
    // LRU eviction
    mutable std::mutex eviction_mutex_;
    std::condition_variable eviction_cv_;
    std::thread eviction_thread_;
    std::atomic<bool> running_{true};
    
    // Statistics
    mutable std::atomic<uint64_t> hits_{0};
    mutable std::atomic<uint64_t> misses_{0};
    mutable std::atomic<uint64_t> evictions_{0};
    
    // Internal methods
    bool should_evict() const;
    void evict_lru();
    void eviction_worker();
    void update_access_stats(CacheEntry* entry);
    uint64_t get_current_time() const;
};

// Memory-mapped SSTable index
class MMapIndex {
public:
    explicit MMapIndex(const std::string& index_file_path);
    ~MMapIndex();
    
    bool open();
    void close();
    
    // Index lookup
    struct IndexEntry {
        Key key;
        uint64_t offset;
        uint32_t size;
        uint32_t checksum;
    };
    
    bool find(const Key& key, IndexEntry& entry) const;
    bool get_range(const Key& start_key, const Key& end_key, 
                   std::vector<IndexEntry>& entries) const;
    
    // Statistics
    size_t num_entries() const { return num_entries_; }
    size_t file_size() const { return file_size_; }
    bool is_mapped() const { return mapped_data_ != nullptr; }
    
private:
    std::string index_file_path_;
    int fd_;
    void* mapped_data_;
    size_t file_size_;
    size_t num_entries_;
    
    // Index layout
    struct IndexHeader {
        uint64_t magic;
        uint32_t version;
        uint64_t num_entries;
        uint64_t data_offset;
        uint32_t checksum;
    };
    
    const IndexHeader* header_;
    const IndexEntry* entries_;
    
    bool validate_header();
    const IndexEntry* find_entry(const Key& key) const;
    size_t binary_search(const Key& key) const;
};

// Enhanced SSTable Reader with bloom filters and mmap
class EnhancedSSTableReader {
public:
    explicit EnhancedSSTableReader(const std::string& sstable_path);
    ~EnhancedSSTableReader();
    
    bool open();
    void close();
    
    // Read operations
    bool get(const Key& key, Value& value);
    bool scan(const Key& start_key, const Key& end_key, 
              std::vector<std::pair<Key, Value>>& results);
    
    // SSTable metadata
    uint64_t sstable_id() const { return sstable_id_; }
    size_t file_size() const { return file_size_; }
    uint64_t entry_count() const { return entry_count_; }
    uint32_t level() const { return level_; }
    
    // Bloom filter access
    bool key_might_exist(const Key& key) const;
    
    // Statistics
    struct Stats {
        uint64_t reads;
        uint64_t bloom_filter_hits;
        uint64_t bloom_filter_misses;
        uint64_t disk_reads;
        uint64_t cache_hits;
        double avg_read_latency_us;
    };
    
    Stats get_stats() const;
    void reset_stats();
    
private:
    std::string sstable_path_;
    std::string data_file_path_;
    std::string index_file_path_;
    std::string bloom_file_path_;
    
    int data_fd_;
    uint64_t sstable_id_;
    uint32_t level_;
    size_t file_size_;
    uint64_t entry_count_;
    
    // Components
    std::unique_ptr<MMapIndex> index_;
    std::unique_ptr<OptimizedBloomFilter> bloom_filter_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;
    
    // Internal methods
    bool read_data_block(uint64_t offset, uint32_t size, std::vector<char>& buffer);
    bool parse_data_entry(const std::vector<char>& buffer, 
                         size_t offset, Key& key, Value& value);
    void update_read_stats(bool bloom_hit, bool disk_read, uint64_t latency_us);
};

// Tiered Compaction Strategy
enum class CompactionStrategy {
    SIZE_TIERED,    // Merge similar-sized SSTables
    LEVELED,        // Leveled compaction like LevelDB
    ADAPTIVE        // Adaptive strategy based on workload
};

class TieredCompactionManager {
public:
    struct CompactionTask {
        uint32_t level;
        std::vector<uint64_t> sstable_ids;
        double priority_score;
        uint64_t estimated_input_bytes;
        uint64_t estimated_output_bytes;
        
        bool operator<(const CompactionTask& other) const {
            return priority_score < other.priority_score; // Min-heap
        }
    };
    
    struct CompactionStats {
        uint64_t total_compactions;
        uint64_t bytes_compacted;
        uint64_t input_files;
        uint64_t output_files;
        double avg_compaction_time_ms;
        uint64_t write_amplification;
        uint64_t read_amplification;
    };
    
    explicit TieredCompactionManager(ShardManager* shard_manager,
                                   CompactionStrategy strategy = CompactionStrategy::ADAPTIVE);
    ~TieredCompactionManager();
    
    // Compaction management
    void start();
    void stop();
    
    // SSTable management
    void register_sstable(uint64_t sstable_id, uint32_t level, size_t size);
    void unregister_sstable(uint64_t sstable_id);
    
    // Compaction scheduling
    void schedule_compaction();
    std::vector<CompactionTask> get_compaction_candidates();
    
    // Strategy selection
    void set_strategy(CompactionStrategy strategy);
    CompactionStrategy get_strategy() const { return strategy_; }
    
    // Statistics
    CompactionStats get_stats() const;
    void reset_stats();
    
    // Configuration
    struct Config {
        // Size-tiered settings
        size_t size_tier_threshold = 4; // Max SSTables per tier
        double size_ratio = 2.0;       // Size ratio between tiers
        
        // Leveled settings
        size_t level_multiplier = 10;   // Size multiplier per level
        size_t max_levels = 7;          // Maximum number of levels
        
        // Adaptive settings
        double read_write_ratio_threshold = 0.1; // Switch threshold
        uint32_t compaction_interval_seconds = 60;
        
        // Performance targets
        double max_write_impact = 0.05;  // Max 5% write impact
        uint32_t max_concurrent_compactions = 2;
    };
    
    void update_config(const Config& config);
    Config get_config() const { return config_; }
    
private:
    ShardManager* shard_manager_;
    CompactionStrategy strategy_;
    Config config_;
    
    // SSTable tracking
    struct SSTableInfo {
        uint64_t sstable_id;
        uint32_t level;
        size_t size;
        uint64_t creation_time;
        uint64_t last_access_time;
        uint32_t read_count;
        uint32_t write_count;
    };
    
    std::unordered_map<uint64_t, SSTableInfo> sstables_;
    mutable std::shared_mutex sstables_mutex_;
    
    // Compaction state
    std::atomic<bool> running_{false};
    std::thread compaction_thread_;
    std::vector<std::thread> worker_threads_;
    
    // Compaction queue
    std::priority_queue<CompactionTask> compaction_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    CompactionStats stats_;
    
    // Internal methods
    void compaction_worker();
    void process_compaction_task(const CompactionTask& task);
    
    // Strategy-specific methods
    std::vector<CompactionTask> get_size_tiered_candidates();
    std::vector<CompactionTask> get_leveled_candidates();
    std::vector<CompactionTask> get_adaptive_candidates();
    
    // Compaction execution
    bool execute_compaction(const CompactionTask& task);
    bool merge_sstables(const std::vector<uint64_t>& sstable_ids, 
                       uint64_t new_sstable_id);
    
    // Priority calculation
    double calculate_compaction_priority(const std::vector<uint64_t>& sstable_ids);
    void update_compaction_stats(const CompactionTask& task, bool success, 
                                uint64_t duration_ms);
    
    // Adaptive strategy
    CompactionStrategy select_optimal_strategy();
    void monitor_workload_characteristics();
};

// Read Path Coordinator
class ReadPathCoordinator {
public:
    explicit ReadPathCoordinator(ShardManager* shard_manager,
                                TieredCompactionManager* compaction_manager,
                                size_t row_cache_size = 1024 * 1024 * 1024);
    ~ReadPathCoordinator();
    
    bool start();
    void stop();
    
    // Main read operations
    bool get(const Key& key, Value& value);
    bool scan(const Key& start_key, const Key& end_key, 
              std::vector<std::pair<Key, Value>>& results);
    
    // Batch operations
    bool multi_get(const std::vector<Key>& keys, 
                   std::vector<std::pair<Key, Value>>& results);
    
    // Cache management
    GlobalRowCache& row_cache() { return *row_cache_; }
    
    // Read path statistics
    struct ReadStats {
        uint64_t total_reads;
        uint64_t cache_hits;
        uint64_t cache_misses;
        uint64_t bloom_filter_hits;
        uint64_t bloom_filter_misses;
        uint64_t disk_reads;
        uint64_t sstables_checked;
        double avg_read_latency_us;
        double cache_hit_rate;
    };
    
    ReadStats get_stats() const;
    void reset_stats();
    
private:
    ShardManager* shard_manager_;
    TieredCompactionManager* compaction_manager_;
    std::unique_ptr<GlobalRowCache> row_cache_;
    
    // SSTable readers cache
    std::unordered_map<uint64_t, std::unique_ptr<EnhancedSSTableReader>> sstable_readers_;
    mutable std::shared_mutex readers_mutex_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    ReadStats stats_;
    
    // Internal methods
    bool read_from_memtable(const Key& key, Value& value);
    bool read_from_sstables(const Key& key, Value& value);
    EnhancedSSTableReader* get_sstable_reader(uint64_t sstable_id);
    void update_read_stats(bool cache_hit, bool bloom_hit, bool disk_read, 
                          uint64_t latency_us, uint32_t sstables_checked);
    
    // Read optimization
    void preload_hot_keys();
    void optimize_read_order(std::vector<Key>& keys);
};

} // namespace nscfstore
