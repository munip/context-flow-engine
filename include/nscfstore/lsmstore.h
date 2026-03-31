#pragma once

#include "common.h"
#include "memory_pool.h"
#include "shard.h"
#include "network.h"
#include "relationship.h"
#include <memory>
#include <string>
#include <atomic>

namespace nscfstore {

class LSMStore {
public:
    explicit LSMStore(const std::string& config_file = "");
    ~LSMStore();
    
    bool start();
    void stop();
    bool is_running() const { return running_.load(); }
    
    // Direct API (for testing/embedded usage)
    bool put(const Key& key, const Value& value);
    bool get(const Key& key, Value& value);
    bool remove(const Key& key);
    std::vector<std::pair<Key, Value>> scan(const Key& start_key, const Key& end_key);
    
    // Component access
    ShardManager& shard_manager() { return *shard_manager_; }
    NetworkServer& network_server() { return *network_server_; }
    RelationshipManager& relationship_manager() { return *relationship_manager_; }
    
    // Statistics
    struct GlobalStats {
        uint64_t total_requests;
        uint64_t total_bytes_read;
        uint64_t total_bytes_written;
        double avg_latency_ms;
        uint64_t active_connections;
        uint64_t total_sstables;
        size_t total_memory_used;
        size_t total_memory_capacity;
    };
    
    GlobalStats get_stats() const;
    
private:
    std::string config_file_;
    std::atomic<bool> running_{false};
    
    // Simple in-memory storage for now
    std::unordered_map<Key, Value> storage_;
    
    std::unique_ptr<ShardManager> shard_manager_;
    std::unique_ptr<NetworkServer> network_server_;
    std::unique_ptr<RelationshipManager> relationship_manager_;
    
    bool load_config();
    bool setup_directories();
    void cleanup();
};

} // namespace nscfstore
