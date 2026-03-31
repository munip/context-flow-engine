#pragma once

#include "proto/context_engine.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <chrono>

namespace nscfstore {

// Simplified gRPC client implementation
class ContextEngineClient {
public:
    explicit ContextEngineClient(const std::string& server_address = "localhost:50051");
    ~ContextEngineClient();
    
    // Connection management
    bool Connect();
    void Disconnect();
    bool IsConnected() const;
    
    // Query operations
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
    
    // Health check
    bool HealthCheck(const std::string& service_name,
                    bool detailed,
                    context_engine::HealthCheckResponse& response);
    
    // Streaming ingestion (simplified)
    bool IngestContext_Stream(const std::vector<context_engine::ContextEvent>& events);
    
private:
    std::string server_address_;
    std::shared_ptr<grpc::Channel> channel_;
    std::atomic<bool> connected_{false};
    
    // Connection management
    bool EnsureConnection();
    void HandleConnectionError(const std::string& error);
};

} // namespace nscfstore
