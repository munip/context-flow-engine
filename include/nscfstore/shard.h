#pragma once

#include "common.h"
#include "memory_pool.h"
#include <thread>
#include <atomic>
#include <queue>
#include <condition_variable>

namespace nscfstore {

class Memtable;
class WAL;
class SSTableManager;

class Shard {
public:
    explicit Shard(uint32_t shard_id, uint32_t total_shards);
    ~Shard();
    
    void start();
    void stop();
    void join();
    
    // Request processing
    void enqueue_request(std::unique_ptr<Request> request);
    std::unique_ptr<Response> execute_request(const Request& request);
    
    // Component access
    Memtable& memtable() { return *memtable_; }
    WAL& wal() { return *wal_; }
    SSTableManager& sstable_manager() { return *sstable_manager_; }
    ShardMemoryPool& memory_pool() { return *memory_pool_; }
    const ShardMemoryPool& memory_pool() const { return *memory_pool_; }
    
    // Statistics access
    uint64_t get_requests_processed() const { return requests_processed_.load(); }
    uint64_t get_bytes_read() const { return bytes_read_.load(); }
    uint64_t get_bytes_written() const { return bytes_written_.load(); }
    
    uint32_t shard_id() const { return shard_id_; }
    bool is_running() const { return running_.load(); }
    
private:
    void run();
    void process_requests();
    void handle_compaction();
    void maintenance_tasks();
    
    uint32_t shard_id_;
    uint32_t total_shards_;
    std::atomic<bool> running_;
    std::thread worker_thread_;
    
    // Request queue
    std::queue<std::unique_ptr<Request>> request_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    
    // Components
    std::unique_ptr<ShardMemoryPool> memory_pool_;
    std::unique_ptr<Memtable> memtable_;
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<SSTableManager> sstable_manager_;
    
    // Statistics
    std::atomic<uint64_t> requests_processed_{0};
    std::atomic<uint64_t> bytes_written_{0};
    std::atomic<uint64_t> bytes_read_{0};
};

class ShardManager {
public:
    explicit ShardManager(uint32_t num_shards = 0);
    ~ShardManager();
    
    void start_all();
    void stop_all();
    
    Shard* get_shard(const Key& key);
    Shard* get_shard_by_id(uint32_t shard_id);
    
    uint32_t num_shards() const { return shards_.size(); }
    
    // Request routing
    void route_request(std::unique_ptr<Request> request);
    
    // Statistics
    struct Stats {
        uint64_t total_requests;
        uint64_t total_bytes_read;
        uint64_t total_bytes_written;
        double avg_latency_ms;
    };
    
    Stats get_global_stats() const;
    
private:
    uint32_t num_shards_;
    std::vector<std::unique_ptr<Shard>> shards_;
    
    void detect_cpu_cores();
    void pin_thread_to_core(std::thread& thread, uint32_t core_id);
};

} // namespace nscfstore
