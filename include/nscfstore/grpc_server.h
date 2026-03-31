#pragma once

#include "common.h"
#include "proto/context_engine.pb.h"
#include "nscfstore/lsmstore.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <atomic>
#include <thread>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <map>
#include <vector>
#include <functional>

namespace nscfstore {

// Consistent hashing for shard routing
class ConsistentHashRouter {
public:
    explicit ConsistentHashRouter(uint32_t num_shards) : num_shards_(num_shards) {
        // Initialize ring with virtual nodes
        for (uint32_t shard_id = 0; shard_id < num_shards_; ++shard_id) {
            for (uint32_t i = 0; i < virtual_nodes_per_shard_; ++i) {
                std::string virtual_node = "shard_" + std::to_string(shard_id) + "_node_" + std::to_string(i);
                uint64_t hash = HashKey(virtual_node);
                ring_[hash] = shard_id;
            }
        }
    }
    
    uint32_t GetShard(const std::string& key) const {
        if (ring_.empty()) return 0;
        
        uint64_t hash = HashKey(key);
        auto it = ring_.lower_bound(hash);
        
        if (it == ring_.end()) {
            return ring_.begin()->second; // Wrap around
        }
        
        return it->second;
    }
    
    std::vector<uint32_t> GetAllShards() const {
        std::vector<uint32_t> shards;
        for (uint32_t i = 0; i < num_shards_; ++i) {
            shards.push_back(i);
        }
        return shards;
    }
    
private:
    uint64_t HashKey(const std::string& key) const {
        // Simple hash function - in production use a better one
        uint64_t hash = 5381;
        for (char c : key) {
            hash = ((hash << 5) + hash) + c;
        }
        return hash;
    }
    
    std::map<uint64_t, uint32_t> ring_;
    uint32_t num_shards_;
    uint32_t virtual_nodes_per_shard_ = 150; // Recommended for even distribution
};

// Real gRPC server implementation with streaming and backpressure
class ContextEngineServiceImpl {
public:
    explicit ContextEngineServiceImpl(LSMStore* lsm_store);
    ~ContextEngineServiceImpl();
    
    // gRPC service implementations (simplified for now)
    bool IngestContext_Stream(const std::string& client_id, 
                              const std::vector<context_engine::ContextEvent>& events);
    
    bool QueryRelationship(const std::string& node_id,
                        const std::vector<std::string>& relationship_types,
                        bool include_inferred,
                        uint32_t max_results,
                        context_engine::RelationshipQueryResponse& response);
    
    bool GetContextSubGraph(const std::string& root_node_id,
                           uint32_t depth,
                           const std::vector<std::string>& relationship_types,
                           uint32_t max_nodes,
                           bool include_attributes,
                           bool include_inferred,
                           context_engine::SubGraphQueryResponse& response);
    
    bool HealthCheck(const std::string& service_name,
                    bool detailed,
                    context_engine::HealthCheckResponse& response);
    
    // Monitoring and statistics
    void UpdateSystemMetrics(); // Made public for server access
    bool IsSystemHealthy();

private:
    // Backpressure management
    struct BackpressureConfig {
        uint32_t max_queue_depth = 10000;
        uint32_t max_compaction_backlog = 100;
        double max_memtable_utilization = 0.8;
        uint32_t flow_control_window_size = 1000;
        uint32_t backpressure_threshold_ms = 100;
    };

private:
    // Core components
    std::unique_ptr<ConsistentHashRouter> router_;
    BackpressureConfig backpressure_config_;
    LSMStore* lsm_store_; // Real LSM engine
    
    // System state
    std::atomic<uint32_t> current_queue_depth_{0};
    std::atomic<uint32_t> compaction_backlog_{0};
    std::atomic<double> memtable_utilization_{0.0};
    std::atomic<bool> accepting_requests_{true};
    
    // Storage (simplified for now)
    std::map<std::string, context_engine::ContextNode> context_store_;
    std::map<std::string, std::vector<context_engine::Relationship>> relationship_store_;
    std::mutex storage_mutex_;
    
    // Internal methods
    uint32_t RouteToShard(const std::string& entity_id);
    bool ShouldApplyBackpressure();
    
    // Request processing
    bool ProcessContextEvent(const context_engine::ContextEvent& event);
    context_engine::RelationshipQueryResponse ProcessRelationshipQuery(
        const std::string& node_id,
        const std::vector<std::string>& relationship_types,
        bool include_inferred,
        uint32_t max_results);
    context_engine::SubGraphQueryResponse ProcessSubGraphQuery(
        const std::string& root_node_id,
        uint32_t depth,
        const std::vector<std::string>& relationship_types,
        uint32_t max_nodes,
        bool include_attributes,
        bool include_inferred);
    
private:
};

// gRPC Server wrapper
class GrpcContextEngineServer {
public:
    explicit GrpcContextEngineServer(LSMStore* lsm_store, 
                                   const std::string& server_address = "0.0.0.0:50051",
                                   uint32_t num_worker_threads = 4);
    ~GrpcContextEngineServer();
    
    // Server lifecycle
    bool Start();
    void Stop();
    void Wait();
    
    // Health check
    bool IsRunning() const { return running_.load(); }
    std::string GetServerAddress() const { return server_address_; }
    
private:
    std::string server_address_;
    uint32_t num_worker_threads_;
    std::atomic<bool> running_{false};
    
    std::unique_ptr<ContextEngineServiceImpl> service_;
    std::unique_ptr<grpc::Server> server_;
    
    // Health check service
    std::unique_ptr<grpc::HealthCheckServiceInterface> health_check_service_;
    std::unique_ptr<grpc::ServerCompletionQueue> completion_queue_;
};

} // namespace nscfstore
