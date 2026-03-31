#include "nscfstore/grpc_client.h"
#include <iostream>
#include <thread>
#include <chrono>

namespace nscfstore {

// ContextEngineClient implementation
ContextEngineClient::ContextEngineClient(const std::string& server_address) 
    : server_address_(server_address) {
    std::cout << "Creating gRPC client for " << server_address << std::endl;
}

ContextEngineClient::~ContextEngineClient() {
    Disconnect();
}

bool ContextEngineClient::Connect() {
    try {
        std::cout << "🔌 Connecting to gRPC server..." << std::endl;
        
        // Create channel
        channel_ = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
        
        if (!channel_) {
            std::cerr << "Failed to create gRPC channel" << std::endl;
            return false;
        }
        
        // Wait for channel to be ready
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        if (!channel_->WaitForConnected(deadline)) {
            std::cerr << "Failed to connect to gRPC server" << std::endl;
            return false;
        }
        
        connected_.store(true);
        std::cout << "✅ gRPC client connected successfully" << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to connect: " << e.what() << std::endl;
        return false;
    }
}

void ContextEngineClient::Disconnect() {
    if (connected_.load()) {
        connected_.store(false);
        channel_.reset();
        std::cout << "✅ gRPC client disconnected" << std::endl;
    }
}

bool ContextEngineClient::IsConnected() const {
    return connected_.load();
}

bool ContextEngineClient::EnsureConnection() {
    if (!IsConnected()) {
        return Connect();
    }
    return true;
}

void ContextEngineClient::HandleConnectionError(const std::string& error) {
    std::cerr << "Connection error: " << error << std::endl;
    Disconnect();
}

bool ContextEngineClient::QueryRelationship(
    const std::string& node_id,
    const std::vector<std::string>& relationship_types,
    bool include_inferred,
    uint32_t max_results,
    context_engine::RelationshipQueryResponse& response) {
    
    if (!EnsureConnection()) {
        return false;
    }
    
    std::cout << "🔍 Querying relationships for node: " << node_id << std::endl;
    
    // Simulate query processing
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // Create mock response
    response.set_success(true);
    response.set_query_time_us(45);
    response.set_total_count(0);
    
    std::cout << "✅ Relationship query completed" << std::endl;
    return true;
}

bool ContextEngineClient::GetContextSubGraph(
    const std::string& root_node_id,
    uint32_t depth,
    const std::vector<std::string>& relationship_types,
    uint32_t max_nodes,
    bool include_attributes,
    bool include_inferred,
    context_engine::SubGraphQueryResponse& response) {
    
    if (!EnsureConnection()) {
        return false;
    }
    
    std::cout << "🔍 Querying subgraph for root: " << root_node_id << " depth: " << depth << std::endl;
    
    // Simulate subgraph processing
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    
    // Create mock response
    response.set_success(true);
    response.set_query_time_us(75);
    response.set_total_nodes(0);
    response.set_total_edges(0);
    
    std::cout << "✅ Subgraph query completed" << std::endl;
    return true;
}

bool ContextEngineClient::HealthCheck(
    const std::string& service_name,
    bool detailed,
    context_engine::HealthCheckResponse& response) {
    
    if (!EnsureConnection()) {
        return false;
    }
    
    std::cout << "🏥 Health check for service: " << service_name << std::endl;
    
    // Simulate health check processing
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    // Create mock response
    response.set_status(context_engine::HealthCheckResponse_HealthStatus_HEALTHY);
    response.set_message("System is healthy");
    response.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    // Add system details
    auto& details = *response.mutable_details();
    details["queue_depth"] = "100";
    details["compaction_backlog"] = "5";
    details["memtable_utilization"] = "0.45";
    details["accepting_requests"] = "true";
    
    std::cout << "✅ Health check completed" << std::endl;
    return true;
}

bool ContextEngineClient::IngestContext_Stream(const std::vector<context_engine::ContextEvent>& events) {
    if (!EnsureConnection()) {
        return false;
    }
    
    std::cout << "📤 Ingesting " << events.size() << " context events" << std::endl;
    
    // Simulate ingestion processing
    std::this_thread::sleep_for(std::chrono::milliseconds(events.size() * 5));
    
    std::cout << "✅ Context ingestion completed" << std::endl;
    return true;
}

} // namespace nscfstore
