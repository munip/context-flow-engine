#pragma once

#include "common.h"
#include "memory_pool.h"
#include <atomic>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <liburing.h>
#include <random>
#include <fcntl.h>
#include <unistd.h>

namespace nscfstore {

// High-performance WAL with direct I/O
class DirectIOWAL {
public:
    explicit DirectIOWAL(const std::string& file_path, 
                         size_t buffer_size = 64 * 1024 * 1024, // 64MB buffer
                         size_t segment_size = 256 * 1024 * 1024); // 256MB segments
    ~DirectIOWAL();
    
    bool open();
    void close();
    
    // High-performance write operations
    bool write_record(const void* data, size_t size, uint64_t sequence);
    bool write_batch(const std::vector<std::pair<const void*, size_t>>& records, 
                    const std::vector<uint64_t>& sequences);
    
    // Asynchronous operations with io_uring
    bool write_record_async(const void* data, size_t size, uint64_t sequence);
    bool flush_async();
    bool sync_async();
    
    // Synchronous operations
    bool flush();
    bool sync();
    
    // WAL management
    uint64_t get_last_sequence() const { return last_sequence_.load(); }
    uint64_t get_flushed_sequence() const { return flushed_sequence_.load(); }
    size_t get_file_size() const;
    
    // Statistics
    struct Stats {
        uint64_t total_writes;
        uint64_t total_bytes_written;
        uint64_t total_flushes;
        uint64_t total_syncs;
        double avg_write_latency_us;
        double avg_flush_latency_us;
        uint64_t write_errors;
        uint64_t flush_errors;
    };
    
    Stats get_stats() const;
    void reset_stats();
    
private:
    std::string file_path_;
    int fd_;
    size_t buffer_size_;
    size_t segment_size_;
    
    // Direct I/O buffer (aligned)
    void* write_buffer_;
    size_t buffer_offset_;
    std::mutex buffer_mutex_;
    
    // Sequence numbers
    std::atomic<uint64_t> last_sequence_{0};
    std::atomic<uint64_t> flushed_sequence_{0};
    
    // File management
    uint64_t current_file_offset_;
    uint32_t current_segment_;
    
    // io_uring for async operations
    io_uring ring_;
    std::atomic<bool> io_uring_initialized_{false};
    
    // Background flush thread
    std::thread flush_thread_;
    std::atomic<bool> running_{false};
    std::condition_variable flush_cv_;
    std::mutex flush_mutex_;
    std::atomic<bool> flush_pending_{false};
    
    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;
    
    // Internal methods
    bool init_io_uring();
    bool setup_direct_io();
    void* allocate_aligned_buffer(size_t size);
    void free_aligned_buffer(void* buffer);
    
    bool write_to_buffer(const void* data, size_t size);
    bool flush_buffer();
    bool rotate_segment();
    
    void flush_worker();
    void update_write_stats(size_t bytes, uint64_t latency_us);
    void update_flush_stats(uint64_t latency_us);
    
    // Alignment helpers
    static constexpr size_t ALIGNMENT = 4096; // 4KB alignment for direct I/O
    static size_t align_up(size_t size, size_t alignment);
    static void* align_pointer(void* ptr, size_t alignment);
};

// Lock-free SkipList node for Memtable
template<typename K, typename V>
class SkipListNode {
public:
    K key;
    V value;
    std::atomic<SkipListNode*> next[MAX_SKIP_LIST_LEVELS];
    std::atomic<bool> marked{false};
    uint8_t level;
    
    SkipListNode(const K& k, const V& v, uint8_t lvl) 
        : key(k), value(v), level(lvl) {
        for (int i = 0; i <= lvl; ++i) {
            next[i].store(nullptr);
        }
    }
};

// Lock-free SkipList Memtable
template<typename K, typename V>
class LockFreeMemtable {
public:
    explicit LockFreeMemtable(ShardMemoryPool* pool, size_t threshold_bytes = 128 * 1024 * 1024);
    ~LockFreeMemtable();
    
    // Core operations
    bool put(const K& key, const V& value);
    bool get(const K& key, V& value);
    bool remove(const K& key);
    
    // Iterator for range scans and flushing
    class Iterator {
    public:
        Iterator(LockFreeMemtable* memtable, const K& start_key = K{});
        bool next();
        const K& key() const { return current_->key; }
        const V& value() const { return current_->value; }
        bool is_valid() const { return current_ != nullptr; }
        
    private:
        SkipListNode<K, V>* current_;
        LockFreeMemtable* memtable_;
        K start_key_;
        SkipListNode<K, V>* find_start_node(const K& key);
    };
    
    Iterator begin(const K& start_key = K{}) { return Iterator(this, start_key); }
    
    // Memtable management
    size_t size_bytes() const { return size_bytes_.load(); }
    size_t entry_count() const { return entry_count_.load(); }
    bool is_full() const { return size_bytes_.load() >= threshold_bytes_; }
    bool is_immutable() const { return immutable_.load(); }
    
    void mark_immutable() { immutable_.store(true); }
    void reset();
    
    // For flushing to SSTable
    std::vector<std::pair<K, V>> get_all_entries();
    std::vector<std::pair<K, V>> get_range(const K& start_key, const K& end_key);
    
private:
    SkipListNode<K, V>* head_;
    std::atomic<int> max_level_;
    std::atomic<size_t> size_bytes_{0};
    std::atomic<size_t> entry_count_{0};
    std::atomic<bool> immutable_{false};
    
    ShardMemoryPool* memory_pool_;
    size_t threshold_bytes_;
    
    // Random number generation for level selection
    std::mt19937 rng_;
    std::geometric_distribution<int> level_dist_;
    
    // Memory management
    std::atomic<SkipListNode<K, V>*> garbage_list_{nullptr};
    
    // Internal methods
    int random_level();
    SkipListNode<K, V>* find_predecessors(const K& key, 
                                         std::vector<SkipListNode<K, V>*>& preds,
                                         std::vector<SkipListNode<K, V>*>& succs);
    
    bool mark_for_removal(SkipListNode<K, V>* node,
                         std::vector<SkipListNode<K, V>*>& preds,
                         std::vector<SkipListNode<K, V>*>& succs);
    
    void add_to_garbage(SkipListNode<K, V>* node);
    void cleanup_garbage();
    
    // Epoch-based reclamation
    struct Epoch {
        std::atomic<uint64_t> epoch{0};
        std::vector<SkipListNode<K, V>*> retired_nodes;
        std::mutex mutex;
    };
    
    std::array<Epoch, 3> epochs_;
    std::atomic<int> current_epoch_{0};
    void enter_epoch();
    void leave_epoch();
    void advance_epoch();
};

// Write path coordinator
class WritePathCoordinator {
public:
    explicit WritePathCoordinator(const std::string& wal_path,
                                 ShardMemoryPool* pool,
                                 size_t memtable_threshold = 128 * 1024 * 1024);
    ~WritePathCoordinator();
    
    bool start();
    void stop();
    
    // Main write operation
    bool put(const Key& key, const Value& value);
    
    // Batch write operations
    bool put_batch(const std::vector<std::pair<Key, Value>>& entries);
    
    // Flush management
    void trigger_flush();
    bool flush_in_progress() const { return flush_in_progress_.load(); }
    
    // Statistics
    struct WriteStats {
        uint64_t total_puts;
        uint64_t total_bytes_written;
        double avg_put_latency_us;
        uint64_t memtable_flushes;
        uint64_t wal_writes;
        uint64_t wal_syncs;
        double wal_write_latency_us;
        double wal_sync_latency_us;
    };
    
    WriteStats get_stats() const;
    void reset_stats();
    
    // Component access
    DirectIOWAL& wal() { return *wal_; }
    LockFreeMemtable<Key, Value>& active_memtable() { return *active_memtable_; }
    
private:
    std::string wal_path_;
    ShardMemoryPool* memory_pool_;
    size_t memtable_threshold_;
    
    std::atomic<bool> running_{false};
    
    // Components
    std::unique_ptr<DirectIOWAL> wal_;
    std::unique_ptr<LockFreeMemtable<Key, Value>> active_memtable_;
    std::unique_ptr<LockFreeMemtable<Key, Value>> immutable_memtable_;
    
    // Flush management
    std::atomic<bool> flush_in_progress_{false};
    std::thread flush_thread_;
    std::condition_variable flush_cv_;
    std::mutex flush_mutex_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    WriteStats stats_;
    
    // Internal methods
    bool write_to_wal(const Key& key, const Value& value, uint64_t sequence);
    bool write_to_memtable(const Key& key, const Value& value);
    
    void flush_worker();
    bool flush_memtable_to_sstable();
    void switch_memtables();
    
    void update_put_stats(size_t bytes, uint64_t latency_us);
    void update_flush_stats();
};

// SSTable Level 0 builder from memtable
class SSTableLevel0Builder {
public:
    explicit SSTableLevel0Builder(const std::string& sstable_path,
                                 ShardMemoryPool* pool);
    ~SSTableLevel0Builder();
    
    bool build_from_memtable(LockFreeMemtable<Key, Value>* memtable);
    bool build_from_entries(const std::vector<std::pair<Key, Value>>& entries);
    
    std::string get_sstable_path() const { return sstable_path_; }
    uint64_t get_entry_count() const { return entry_count_; }
    size_t get_file_size() const;
    
private:
    std::string sstable_path_;
    ShardMemoryPool* memory_pool_;
    int fd_;
    uint64_t entry_count_;
    
    // SSTable format
    struct SSTableHeader {
        uint64_t magic;
        uint32_t version;
        uint64_t entry_count;
        uint64_t data_offset;
        uint64_t index_offset;
        uint64_t footer_offset;
        uint32_t checksum;
    };
    
    struct IndexEntry {
        Key key;
        uint64_t offset;
        uint32_t size;
        uint32_t checksum;
    };
    
    bool write_header();
    bool write_data_block(const std::vector<std::pair<Key, Value>>& entries);
    bool write_index_block(const std::vector<IndexEntry>& index);
    bool write_footer();
    
    uint32_t calculate_checksum(const void* data, size_t size);
    bool ensure_directory_exists(const std::string& path);
};

} // namespace nscfstore
