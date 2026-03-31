#include "nscfstore/grpc_server.h"
#include <chrono>
#include <sstream>
#include <algorithm>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/server_builder.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <map>
#include <queue>
#include <set>
#include <cmath>
#include <iomanip>

namespace nscfstore {

// ContextEngineServiceImpl Implementation
ContextEngineServiceImpl::ContextEngineServiceImpl(LSMStore* lsm_store) 
    : lsm_store_(lsm_store) {
    std::cout << "Creating ContextEngineServiceImpl with streaming and backpressure" << std::endl;
    
    // Initialize consistent hash router
    uint32_t num_shards = std::thread::hardware_concurrency();
    router_ = std::make_unique<ConsistentHashRouter>(num_shards);
    
    std::cout << "✓ Initialized with " << num_shards << " shards" << std::endl;
}

ContextEngineServiceImpl::~ContextEngineServiceImpl() {
    // Cleanup
}

bool ContextEngineServiceImpl::IngestContext_Stream(const std::string& client_id, 
                                              const std::vector<context_engine::ContextEvent>& events) {
    std::cout << "Processing " << events.size() << " events from client: " << client_id << std::endl;
    
    // Check backpressure
    if (ShouldApplyBackpressure()) {
        std::cout << "⚠️  Backpressure applied - queue depth: " << current_queue_depth_.load() << std::endl;
        return false;
    }
    
    bool all_success = true;
    for (const auto& event : events) {
        // Route to appropriate shard
        uint32_t shard_id = RouteToShard(event.node().node_id());
        
        // Process event
        bool success = ProcessContextEvent(event);
        if (!success) {
            all_success = false;
            std::cerr << "Failed to process event: " << event.event_id() << std::endl;
        }
        
        // Update metrics
        current_queue_depth_++;
    }
    
    std::cout << "✓ Processed " << events.size() << " events, success: " << all_success << std::endl;
    return all_success;
}

bool ContextEngineServiceImpl::QueryRelationship(const std::string& node_id,
                                            const std::vector<std::string>& relationship_types,
                                            bool include_inferred,
                                            uint32_t max_results,
                                            context_engine::RelationshipQueryResponse& response) {
    std::cout << "Relationship query for node: " << node_id << std::endl;
    
    // Route to appropriate shard
    uint32_t shard_id = RouteToShard(node_id);
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Process query
    auto query_response = ProcessRelationshipQuery(node_id, relationship_types, include_inferred, max_results);
    response = query_response;
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    response.set_query_time_us(duration.count());
    response.set_total_count(query_response.relationships_size());
    response.set_success(true);
    
    std::cout << "✓ Query completed in " << duration.count() << "μs, found " 
              << query_response.relationships_size() << " relationships" << std::endl;
    
    return true;
}

bool ContextEngineServiceImpl::GetContextSubGraph(const std::string& root_node_id,
                                               uint32_t depth,
                                               const std::vector<std::string>& relationship_types,
                                               uint32_t max_nodes,
                                               bool include_attributes,
                                               bool include_inferred,
                                               context_engine::SubGraphQueryResponse& response) {
    std::cout << "Subgraph query for root: " << root_node_id << " depth: " << depth << std::endl;
    
    // Route to appropriate shard
    uint32_t shard_id = RouteToShard(root_node_id);
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Process subgraph query
    auto subgraph_response = ProcessSubGraphQuery(root_node_id, depth, relationship_types, 
                                             max_nodes, include_attributes, include_inferred);
    response = subgraph_response;
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    response.set_query_time_us(duration.count());
    response.set_total_nodes(subgraph_response.subgraph().nodes_size());
    response.set_total_edges(subgraph_response.subgraph().edges_size());
    response.set_success(true);
    
    std::cout << "✓ Subgraph query completed in " << duration.count() << "μs, found " 
              << subgraph_response.subgraph().nodes_size() << " nodes, " 
              << subgraph_response.subgraph().edges_size() << " edges" << std::endl;
    
    return true;
}

bool ContextEngineServiceImpl::HealthCheck(const std::string& service_name,
                                       bool detailed,
                                       context_engine::HealthCheckResponse& response) {
    std::cout << "Health check for service: " << service_name << std::endl;
    
    bool is_healthy = IsSystemHealthy();
    
    response.set_status(is_healthy ? 
        context_engine::HealthCheckResponse_HealthStatus_HEALTHY : 
        context_engine::HealthCheckResponse_HealthStatus_DEGRADED);
    
    response.set_message(is_healthy ? "System is healthy" : "System is degraded");
    response.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    // Add system details
    auto& details = *response.mutable_details();
    details["queue_depth"] = std::to_string(current_queue_depth_.load());
    details["compaction_backlog"] = std::to_string(compaction_backlog_.load());
    details["memtable_utilization"] = std::to_string(memtable_utilization_.load());
    details["accepting_requests"] = accepting_requests_.load() ? "true" : "false";
    
    std::cout << "✓ Health check completed, status: " << (is_healthy ? "HEALTHY" : "DEGRADED") << std::endl;
    return true;
}

// Private methods implementation
uint32_t ContextEngineServiceImpl::RouteToShard(const std::string& entity_id) {
    return router_ ? router_->GetShard(entity_id) : 0;
}

bool ContextEngineServiceImpl::ShouldApplyBackpressure() {
    return (current_queue_depth_.load() > backpressure_config_.max_queue_depth) ||
           (compaction_backlog_.load() > backpressure_config_.max_compaction_backlog) ||
           (memtable_utilization_.load() > backpressure_config_.max_memtable_utilization);
}

bool ContextEngineServiceImpl::ProcessContextEvent(const context_engine::ContextEvent& event) {
    try {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        
        // Store context node
        context_store_[event.node().node_id()] = event.node();
        
        // Store relationships
        for (const auto& rel : event.relationships()) {
            std::string key = rel.source_id() + "->" + rel.target_id();
            relationship_store_[key].push_back(rel);
        }
        
        std::cout << "  Processed context event: " << event.event_id() 
                  << " for node: " << event.node().node_id() << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error processing context event: " << e.what() << std::endl;
        return false;
    }
}

context_engine::RelationshipQueryResponse 
ContextEngineServiceImpl::ProcessRelationshipQuery(const std::string& node_id,
                                                const std::vector<std::string>& relationship_types,
                                                bool include_inferred,
                                                uint32_t max_results) {
    context_engine::RelationshipQueryResponse response;
    response.set_success(true);
    
    std::lock_guard<std::mutex> lock(storage_mutex_);
    
    uint32_t results = 0;
    
    // Find all relationships for this node
    for (const auto& pair : relationship_store_) {
        const auto& rels = pair.second;
        for (const auto& rel : rels) {
            if ((rel.source_id() == node_id || rel.target_id() == node_id) && 
                results < max_results) {
                
                // Filter by relationship types if specified
                if (!relationship_types.empty()) {
                    bool type_matches = false;
                    for (const auto& type : relationship_types) {
                        if (rel.relationship_type() == type) {
                            type_matches = true;
                            break;
                        }
                    }
                    if (!type_matches) continue;
                }
                
                *response.add_relationships() = rel;
                results++;
            }
        }
    }
    
    response.set_total_count(results);
    return response;
}

context_engine::SubGraphQueryResponse 
ContextEngineServiceImpl::ProcessSubGraphQuery(const std::string& root_node_id,
                                             uint32_t depth,
                                             const std::vector<std::string>& relationship_types,
                                             uint32_t max_nodes,
                                             bool include_attributes,
                                             bool include_inferred) {
    context_engine::SubGraphQueryResponse response;
    response.set_success(true);
    
    std::lock_guard<std::mutex> lock(storage_mutex_);
    
    auto* subgraph = response.mutable_subgraph();
    std::set<std::string> visited;
    std::queue<std::string> to_visit;
    
    // Start with root node
    to_visit.push(root_node_id);
    visited.insert(root_node_id);
    
    // BFS traversal
    while (!to_visit.empty() && subgraph->nodes_size() < max_nodes) {
        std::string current = to_visit.front();
        to_visit.pop();
        
        // Add node to subgraph
        if (context_store_.find(current) != context_store_.end()) {
            *subgraph->add_nodes() = context_store_[current];
            (*subgraph->mutable_node_indices())[current] = subgraph->nodes_size() - 1;
        }
        
        // Add edges and queue neighbors
        for (const auto& pair : relationship_store_) {
            const auto& rels = pair.second;
            for (const auto& rel : rels) {
                if (rel.source_id() == current && visited.find(rel.target_id()) == visited.end()) {
                    *subgraph->add_edges() = rel;
                    to_visit.push(rel.target_id());
                    visited.insert(rel.target_id());
                } else if (rel.target_id() == current && visited.find(rel.source_id()) == visited.end()) {
                    *subgraph->add_edges() = rel;
                    to_visit.push(rel.source_id());
                    visited.insert(rel.source_id());
                }
            }
        }
    }
    
    return response;
}

void ContextEngineServiceImpl::UpdateSystemMetrics() {
    // Simulate system metrics
    static uint32_t counter = 0;
    counter++;
    
    // Simulate varying load
    double load = 0.3 + 0.4 * std::sin(counter * 0.1);
    memtable_utilization_.store(load);
    
    // Simulate queue depth
    current_queue_depth_.store(100 + static_cast<uint32_t>(50 * std::sin(counter * 0.05)));
    
    // Simulate compaction backlog
    compaction_backlog_.store(static_cast<uint32_t>(5 + 3 * std::sin(counter * 0.03)));
    
    // Update accepting requests based on system health
    accepting_requests_.store(!ShouldApplyBackpressure());
    
    if (counter % 10 == 0) {
        std::cout << "📊 System metrics - Load: " << std::fixed << std::setprecision(2) << load
                  << " Queue: " << current_queue_depth_.load()
                  << " Backlog: " << compaction_backlog_.load()
                  << " Accepting: " << accepting_requests_.load() << std::endl;
    }
}

bool ContextEngineServiceImpl::IsSystemHealthy() {
    return !ShouldApplyBackpressure();
}

// GrpcContextEngineServer implementation
GrpcContextEngineServer::GrpcContextEngineServer(LSMStore* lsm_store,
                                                   const std::string& server_address,
                                                   uint32_t num_worker_threads)
    : server_address_(server_address), num_worker_threads_(num_worker_threads) {
    service_ = std::make_unique<ContextEngineServiceImpl>(lsm_store);
}

GrpcContextEngineServer::~GrpcContextEngineServer() {
    Stop();
}

bool GrpcContextEngineServer::Start() {
    if (running_.load()) {
        return true;
    }
    
    try {
        std::cout << "🚀 Starting gRPC server on " << server_address_ << std::endl;
        
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
        
        // Set max message sizes
        builder.SetMaxReceiveMessageSize(64 * 1024 * 1024); // 64MB
        builder.SetMaxSendMessageSize(64 * 1024 * 1024);   // 64MB
        
        // Add health check service (simplified)
        // health_check_service_ = std::make_unique<grpc::HealthCheckServiceInterface>();
        // builder.RegisterService(health_check_service_.get());
        
        // Register a simple service to satisfy gRPC requirements
        // We'll use the service implementation directly
        // For now, just build the server without services
        
        // Add a completion queue to satisfy gRPC
        completion_queue_ = builder.AddCompletionQueue();
        
        // Build server
        server_ = builder.BuildAndStart();
        
        if (!server_) {
            std::cerr << "Failed to build gRPC server" << std::endl;
            return false;
        }
        
        running_.store(true);
        std::cout << "✅ gRPC server started successfully on " << server_address_ << std::endl;
        
        // Start monitoring thread
        std::thread([this]() {
            while (running_.load()) {
                service_->UpdateSystemMetrics();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }).detach();
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to start gRPC server: " << e.what() << std::endl;
        return false;
    }
}

void GrpcContextEngineServer::Stop() {
    if (running_.load()) {
        running_.store(false);
        
        if (server_) {
            std::cout << "🛑 Stopping gRPC server..." << std::endl;
            server_->Shutdown();
            std::cout << "✅ gRPC server stopped" << std::endl;
        }
    }
}

void GrpcContextEngineServer::Wait() {
    if (server_) {
        server_->Wait();
    }
}

} // namespace nscfstore
