#include "nscfstore/shard.h"
#include "nscfstore/memtable.h"
#include "nscfstore/wal.h"
#include "nscfstore/sstable.h"
#include <sched.h>
#include <unistd.h>
#include <iostream>

namespace nscfstore {

// Shard Implementation
Shard::Shard(uint32_t shard_id, uint32_t total_shards)
    : shard_id_(shard_id),
      total_shards_(total_shards),
      running_(false) {
    
    // Initialize memory pool (2GB per shard by default)
    const size_t memory_size = 2ULL * 1024 * 1024 * 1024;
    memory_pool_ = std::make_unique<ShardMemoryPool>(shard_id, memory_size);
    
    // Initialize components
    memtable_ = std::make_unique<Memtable>(memory_pool_.get());
    wal_ = std::make_unique<WAL>(shard_id, "./data", memory_pool_.get());
    sstable_manager_ = std::make_unique<SSTableManager>(shard_id, "./data", memory_pool_.get());
}

Shard::~Shard() {
    stop();
}

void Shard::start() {
    if (running_.load()) {
        return;
    }
    
    running_.store(true);
    
    // Start WAL
    if (!wal_->start()) {
        throw std::runtime_error("Failed to start WAL for shard " + std::to_string(shard_id_));
    }
    
    // Start worker thread
    worker_thread_ = std::thread(&Shard::run, this);
}

void Shard::stop() {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    
    // Wake up worker thread
    queue_cv_.notify_all();
    
    // Wait for thread to finish
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    
    // Stop WAL
    wal_->stop();
}

void Shard::join() {
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void Shard::enqueue_request(std::unique_ptr<Request> request) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        request_queue_.push(std::move(request));
    }
    queue_cv_.notify_one();
}

std::unique_ptr<Response> Shard::execute_request(const Request& request) {
    auto response = std::make_unique<Response>();
    response->request_id = request.request_id;
    response->completion_time = std::chrono::steady_clock::now();
    
    try {
        switch (request.type) {
            case OperationType::GET: {
                Value value;
                response->success = memtable_->get(request.key, value);
                if (!response->success) {
                    // Try SSTables
                    response->success = sstable_manager_->get(request.key, value);
                }
                if (response->success) {
                    response->value = value;
                }
                break;
            }
            
            case OperationType::PUT: {
                // Write to WAL first
                if (!wal_->write_put(request.key, request.value)) {
                    response->success = false;
                    response->error = "WAL write failed";
                    break;
                }
                
                // Write to memtable
                response->success = memtable_->put(request.key, request.value);
                if (!response->success) {
                    response->error = "Memtable write failed";
                }
                
                bytes_written_.fetch_add(request.key.size() + request.value.size());
                break;
            }
            
            case OperationType::DELETE: {
                // Write to WAL first
                if (!wal_->write_delete(request.key)) {
                    response->success = false;
                    response->error = "WAL write failed";
                    break;
                }
                
                // Delete from memtable
                response->success = memtable_->remove(request.key);
                if (!response->success) {
                    response->error = "Memtable delete failed";
                }
                break;
            }
            
            case OperationType::SCAN: {
                // For simplicity, just scan memtable for now
                // In production, would need to merge with SSTables
                auto scanner = memtable_->scan("", ""); // Scan all
                std::vector<std::pair<Key, Value>> results;
                
                while (scanner.next()) {
                    results.emplace_back(scanner.key(), scanner.value());
                    // Limit results to prevent memory blowup
                    if (results.size() >= 1000) {
                        break;
                    }
                }
                
                // Serialize results into response value
                std::string serialized;
                for (const auto& [k, v] : results) {
                    serialized += k + "\0" + v + "\0";
                }
                response->value = serialized;
                response->success = true;
                break;
            }
            
            default:
                response->success = false;
                response->error = "Unknown operation type";
                break;
        }
        
        requests_processed_.fetch_add(1);
        
    } catch (const std::exception& e) {
        response->success = false;
        response->error = e.what();
    }
    
    return response;
}

void Shard::run() {
    // Set CPU affinity
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(shard_id_, &cpuset);
    sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
    
    std::cout << "Shard " << shard_id_ << " started on CPU core " << sched_getcpu() << std::endl;
    
    while (running_.load()) {
        process_requests();
        maintenance_tasks();
        
        // Brief sleep to prevent busy waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

void Shard::process_requests() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    
    if (request_queue_.empty()) {
        queue_cv_.wait_for(lock, std::chrono::milliseconds(10));
        return;
    }
    
    auto request = std::move(request_queue_.front());
    request_queue_.pop();
    lock.unlock();
    
    // Process request
    auto response = execute_request(*request);
    
    // For now, we'll just delete the response
    // In a real implementation, we'd send it back to the client
}

void Shard::maintenance_tasks() {
    // Check if memtable needs flushing
    if (memtable_->is_full()) {
        handle_compaction();
    }
    
    // Periodic WAL flush
    static auto last_flush = std::chrono::steady_clock::now();
    auto now = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count() >= 1) {
        wal_->flush();
        last_flush = now;
    }
}

void Shard::handle_compaction() {
    // Get all entries from memtable
    auto entries = memtable_->get_all_entries();
    
    // Create SSTable
    std::vector<uint64_t> sequences(entries.size(), wal_->last_sequence());
    sstable_manager_->create_sstable(entries, sequences);
    
    // Clear memtable
    memtable_->clear();
    
    // Checkpoint WAL
    wal_->checkpoint(wal_->last_sequence());
}

// ShardManager Implementation
ShardManager::ShardManager(uint32_t num_shards) {
    if (num_shards == 0) {
        detect_cpu_cores();
    } else {
        num_shards_ = num_shards;
    }
    
    std::cout << "Initializing ShardManager with " << num_shards_ << " shards" << std::endl;
    
    // Create shards
    shards_.reserve(num_shards_);
    for (uint32_t i = 0; i < num_shards_; ++i) {
        shards_.push_back(std::make_unique<Shard>(i, num_shards_));
    }
}

ShardManager::~ShardManager() {
    stop_all();
}

void ShardManager::start_all() {
    for (auto& shard : shards_) {
        shard->start();
    }
}

void ShardManager::stop_all() {
    for (auto& shard : shards_) {
        shard->stop();
    }
}

Shard* ShardManager::get_shard(const Key& key) {
    uint32_t shard_id = get_shard_id(key, num_shards_);
    return shards_[shard_id].get();
}

Shard* ShardManager::get_shard_by_id(uint32_t shard_id) {
    if (shard_id >= num_shards_) {
        return nullptr;
    }
    return shards_[shard_id].get();
}

void ShardManager::route_request(std::unique_ptr<Request> request) {
    Shard* shard = get_shard(request->key);
    if (shard) {
        shard->enqueue_request(std::move(request));
    }
}

ShardManager::Stats ShardManager::get_global_stats() const {
    Stats stats = {};
    
    for (const auto& shard : shards_) {
        stats.total_requests += shard->get_requests_processed();
        stats.total_bytes_read += shard->get_bytes_read();
        stats.total_bytes_written += shard->get_bytes_written();
    }
    
    // Calculate average latency (simplified)
    stats.avg_latency_ms = 0.5; // Placeholder
    
    return stats;
}

void ShardManager::detect_cpu_cores() {
    num_shards_ = std::thread::hardware_concurrency();
    if (num_shards_ == 0) {
        num_shards_ = 4; // Default fallback
    }
    
    // Limit to maximum supported shards
    num_shards_ = std::min(num_shards_, MAX_SHARDS);
}

void ShardManager::pin_thread_to_core(std::thread& thread, uint32_t core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
}

} // namespace nscfstore {
