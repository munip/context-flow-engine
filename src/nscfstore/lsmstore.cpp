#include "nscfstore/lsmstore.h"
#include <fstream>
#include <iostream>
#include <filesystem>

namespace nscfstore {

nscfstore::LSMStore::LSMStore(const std::string& config_file) 
    : config_file_(config_file), running_(false) {
}

nscfstore::LSMStore::~LSMStore() {
    stop();
}

bool nscfstore::LSMStore::start() {
    std::cout << "LSMStore::start() called" << std::endl;
    if (running_.load()) {
        return true;
    }
    
    try {
        // Setup directories
        if (!setup_directories()) {
            std::cerr << "Failed to setup directories" << std::endl;
            return false;
        }
        
        running_.store(true);
        std::cout << "✅ LSMStore started successfully" << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to start LSMStore: " << e.what() << std::endl;
        return false;
    }
}

void nscfstore::LSMStore::stop() {
    std::cout << "LSMStore::stop() called" << std::endl;
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    cleanup();
    std::cout << "LSMStore stopped successfully" << std::endl;
}

bool nscfstore::LSMStore::load_config() {
    std::cout << "LSMStore::load_config() called" << std::endl;
    return true;
}


bool nscfstore::LSMStore::put(const Key& key, const Value& value) {
    if (!running_.load()) {
        return false;
    }
    
    auto* shard = shard_manager_->get_shard(key);
    if (!shard) {
        return false;
    }
    
    auto request = std::make_unique<Request>();
    request->type = OperationType::PUT;
    request->key = key;
    request->value = value;
    request->start_time = std::chrono::steady_clock::now();
    
    auto response = shard->execute_request(*request);
    return response->success;
}

bool nscfstore::LSMStore::get(const Key& key, Value& value) {
    if (!running_.load()) {
        return false;
    }
    
    auto* shard = shard_manager_->get_shard(key);
    if (!shard) {
        return false;
    }
    
    auto request = std::make_unique<Request>();
    request->type = OperationType::GET;
    request->key = key;
    request->start_time = std::chrono::steady_clock::now();
    
    auto response = shard->execute_request(*request);
    if (response->success) {
        value = response->value;
    }
    
    return response->success;
}

bool nscfstore::LSMStore::remove(const Key& key) {
    if (!running_.load()) {
        return false;
    }
    
    auto* shard = shard_manager_->get_shard(key);
    if (!shard) {
        return false;
    }
    
    auto request = std::make_unique<Request>();
    request->type = OperationType::DELETE;
    request->key = key;
    request->start_time = std::chrono::steady_clock::now();
    
    auto response = shard->execute_request(*request);
    return response->success;
}

std::vector<std::pair<Key, Value>> nscfstore::LSMStore::scan(const Key& start_key, const Key& end_key) {
    std::vector<std::pair<Key, Value>> results;
    
    if (!running_.load()) {
        return results;
    }
    
    // For simplicity, scan from the first shard
    // In a real implementation, would need to coordinate across all shards
    auto* shard = shard_manager_->get_shard_by_id(0);
    if (!shard) {
        return results;
    }
    
    auto request = std::make_unique<Request>();
    request->type = OperationType::SCAN;
    request->key = start_key;
    request->value = end_key;
    request->start_time = std::chrono::steady_clock::now();
    
    auto response = shard->execute_request(*request);
    if (response->success) {
        // Parse the serialized results
        std::istringstream iss(response->value);
        std::string key, value;
        
        while (std::getline(iss, key, '\0')) {
            if (std::getline(iss, value, '\0')) {
                results.emplace_back(key, value);
            }
        }
    }
    
    return results;
}

nscfstore::LSMStore::GlobalStats nscfstore::LSMStore::get_stats() const {
    GlobalStats stats;
    
    if (!running_.load()) {
        return stats;
    }
    
    // Get shard manager stats
    auto shard_stats = shard_manager_->get_global_stats();
    stats.total_requests = shard_stats.total_requests;
    stats.total_bytes_read = shard_stats.total_bytes_read;
    stats.total_bytes_written = shard_stats.total_bytes_written;
    stats.avg_latency_ms = shard_stats.avg_latency_ms;
    
    // Get network server stats (placeholder)
    stats.active_connections = 0;
    
    // Get SSTable stats (placeholder)
    stats.total_sstables = 0;
    
    // Calculate memory usage
    stats.total_memory_used = 0;
    stats.total_memory_capacity = 16ULL * 1024 * 1024 * 1024; // 16GB default
    
    // Add relationship manager stats
    if (relationship_manager_) {
        auto rel_stats = relationship_manager_->get_stats();
        stats.total_requests += rel_stats.total_relationships;
        stats.total_memory_used += rel_stats.memory_usage_bytes;
    }
    
    return stats;
}


bool nscfstore::LSMStore::setup_directories() {
    try {
        std::filesystem::create_directories("./data");
        std::filesystem::create_directories("./data/wal");
        std::filesystem::create_directories("./data/sstable");
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to create directories: " << e.what() << std::endl;
        return false;
    }
}

void nscfstore::LSMStore::cleanup() {
    // Cleanup resources
    shard_manager_.reset();
    network_server_.reset();
    relationship_manager_.reset();
}

} // namespace nscfstore
