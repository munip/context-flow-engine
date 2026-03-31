#include "nscfstore/grpc_client.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace nscfstore;

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << title << std::endl;
    std::cout << std::string(50, '=') << std::endl;
}

void print_stats(const ContextEngineDriver::DriverStats& stats) {
    std::cout << "Driver Statistics:" << std::endl;
    std::cout << "  Total Operations: " << stats.total_operations << std::endl;
    std::cout << "  Successful: " << stats.successful_operations << std::endl;
    std::cout << "  Failed: " << stats.failed_operations << std::endl;
    std::cout << "  Success Rate: " << 
        (stats.total_operations > 0 ? 
         (double)stats.successful_operations / stats.total_operations * 100 : 0) 
        << "%" << std::endl;
    std::cout << "  Average Latency: " << stats.avg_latency_ms << " ms" << std::endl;
    std::cout << "  Reconnections: " << stats.reconnections << std::endl;
    std::cout << "  Connected: " << (stats.is_connected ? "Yes" : "No") << std::endl;
    if (!stats.last_error.empty()) {
        std::cout << "  Last Error: " << stats.last_error << std::endl;
    }
}

int main() {
    print_separator("ContextEngine Quick Start Tutorial");
    
    // 1. Initialize the ContextEngine driver
    std::cout << "1. Initializing ContextEngine driver..." << std::endl;
    
    ContextEngineDriver driver("localhost:50051");
    
    // Configure the driver
    ContextEngineDriver::DriverConfig config;
    config.server_address = "localhost:50051";
    config.connection_retry_interval_ms = 3000;
    config.max_connection_retries = 5;
    config.operation_timeout_ms = 30000;
    config.enable_auto_reconnect = true;
    config.enable_metrics = true;
    
    driver.SetConfig(config);
    
    // Start the driver
    if (!driver.Start()) {
        std::cerr << "Failed to start ContextEngine driver" << std::endl;
        return 1;
    }
    
    std::cout << "✓ Driver started successfully" << std::endl;
    
    // 2. Ingest some context data
    print_separator("2. Ingesting Context Data");
    
    // Create a user context
    std::map<std::string, std::string> user_attributes = {
        {"name", "Alice"},
        {"age", "30"},
        {"department", "Engineering"},
        {"role", "Senior Developer"}
    };
    
    std::vector<std::pair<std::string, std::string>> user_relationships = {
        {"project_alpha", "works_on"},
        {"team_engineering", "member_of"},
        {"bob", "collaborates_with"}
    };
    
    std::cout << "Ingesting user context for 'user_alice'..." << std::endl;
    if (driver.IngestContext("user_alice", "user", user_attributes, user_relationships)) {
        std::cout << "✓ User context ingested successfully" << std::endl;
    } else {
        std::cerr << "✗ Failed to ingest user context" << std::endl;
    }
    
    // Create a project context
    std::map<std::string, std::string> project_attributes = {
        {"name", "Project Alpha"},
        {"type", "software_development"},
        {"status", "active"},
        {"priority", "high"},
        {"start_date", "2024-01-15"}
    };
    
    std::vector<std::pair<std::string, std::string>> project_relationships = {
        {"company_techcorp", "owned_by"},
        {"department_engineering", "managed_by"},
        {"user_alice", "has_contributor"}
    };
    
    std::cout << "Ingesting project context for 'project_alpha'..." << std::endl;
    if (driver.IngestContext("project_alpha", "project", project_attributes, project_relationships)) {
        std::cout << "✓ Project context ingested successfully" << std::endl;
    } else {
        std::cerr << "✗ Failed to ingest project context" << std::endl;
    }
    
    // 3. Query relationships
    print_separator("3. Querying Relationships");
    
    std::cout << "Querying relationships for 'user_alice'..." << std::endl;
    std::vector<std::pair<std::string, std::string>> relationships;
    
    if (driver.GetRelationships("user_alice", relationships)) {
        std::cout << "✓ Found " << relationships.size() << " relationships:" << std::endl;
        for (const auto& [target, type] : relationships) {
            std::cout << "  - " << target << " (" << type << ")" << std::endl;
        }
    } else {
        std::cerr << "✗ Failed to query relationships" << std::endl;
    }
    
    // Query specific relationship type
    std::cout << "\nQuerying 'works_on' relationships for 'user_alice'..." << std::endl;
    std::vector<std::pair<std::string, std::string>> work_relationships;
    
    if (driver.GetRelationships("user_alice", work_relationships, "works_on")) {
        std::cout << "✓ Found " << work_relationships.size() << " work relationships:" << std::endl;
        for (const auto& [target, type] : work_relationships) {
            std::cout << "  - " << target << " (" << type << ")" << std::endl;
        }
    } else {
        std::cerr << "✗ Failed to query work relationships" << std::endl;
    }
    
    // 4. Get context graph
    print_separator("4. Getting Context Graph");
    
    std::cout << "Getting context graph for 'user_alice' (depth=2)..." << std::endl;
    std::vector<std::string> connected_entities;
    std::vector<std::tuple<std::string, std::string, std::string>> edges;
    
    if (driver.GetContextGraph("user_alice", 2, connected_entities, edges)) {
        std::cout << "✓ Context graph retrieved successfully" << std::endl;
        std::cout << "Connected entities (" << connected_entities.size() << "):" << std::endl;
        for (const auto& entity : connected_entities) {
            std::cout << "  - " << entity << std::endl;
        }
        
        std::cout << "\nRelationships (" << edges.size() << "):" << std::endl;
        for (const auto& [source, target, type] : edges) {
            std::cout << "  - " << source << " -> " << target << " (" << type << ")" << std::endl;
        }
    } else {
        std::cerr << "✗ Failed to get context graph" << std::endl;
    }
    
    // 5. Batch operations
    print_separator("5. Batch Operations");
    
    std::cout << "Performing batch context ingestion..." << std::endl;
    
    std::vector<std::tuple<std::string, std::string, std::map<std::string, std::string>>> batch_contexts;
    
    // Add more users
    batch_contexts.emplace_back("user_bob", "user", std::map<std::string, std::string>{
        {"name", "Bob"},
        {"age", "28"},
        {"department", "Engineering"},
        {"role", "Developer"}
    });
    
    batch_contexts.emplace_back("user_charlie", "user", std::map<std::string, std::string>{
        {"name", "Charlie"},
        {"age", "35"},
        {"department", "Product"},
        {"role", "Product Manager"}
    });
    
    batch_contexts.emplace_back("project_beta", "project", std::map<std::string, std::string>{
        {"name", "Project Beta"},
        {"type", "research"},
        {"status", "planning"}
    });
    
    if (driver.BatchIngestContext(batch_contexts)) {
        std::cout << "✓ Batch context ingestion successful (" << batch_contexts.size() << " contexts)" << std::endl;
    } else {
        std::cerr << "✗ Failed to ingest batch contexts" << std::endl;
    }
    
    // 6. Streaming ingestion example
    print_separator("6. Streaming Ingestion");
    
    std::cout << "Demonstrating streaming ingestion..." << std::endl;
    
    // Create a streaming ingestor
    auto ingestor = std::make_unique<ContextEngineDriver::StreamingIngestor>(&driver);
    
    if (ingestor->Start()) {
        std::cout << "✓ Streaming ingestor started" << std::endl;
        
        // Add some contexts via streaming
        for (int i = 0; i < 5; ++i) {
            std::string entity_id = "stream_entity_" + std::to_string(i);
            std::string context_type = "stream_context";
            std::map<std::string, std::string> attributes = {
                {"index", std::to_string(i)},
                {"batch", "demo"},
                {"timestamp", std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count())}
            };
            
            if (ingestor->AddContext(entity_id, context_type, attributes)) {
                std::cout << "✓ Added streaming context: " << entity_id << std::endl;
            } else {
                std::cerr << "✗ Failed to add streaming context: " << entity_id << std::endl;
            }
            
            // Small delay to demonstrate streaming
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        // Get streaming stats
        auto stream_stats = ingestor->GetStats();
        std::cout << "\nStreaming Statistics:" << std::endl;
        std::cout << "  Total Sent: " << stream_stats.total_sent << std::endl;
        std::cout << "  Successful: " << stream_stats.total_success << std::endl;
        std::cout << "  Failed: " << stream_stats.total_failed << std::endl;
        std::cout << "  Current Backlog: " << stream_stats.current_backlog << std::endl;
        std::cout << "  Average Latency: " << stream_stats.avg_latency_ms << " ms" << std::endl;
        
        ingestor->Stop();
        std::cout << "✓ Streaming ingestor stopped" << std::endl;
    } else {
        std::cerr << "✗ Failed to start streaming ingestor" << std::endl;
    }
    
    // 7. Final statistics
    print_separator("7. Final Statistics");
    
    auto final_stats = driver.GetStats();
    print_stats(final_stats);
    
    // 8. Cleanup
    print_separator("8. Cleanup");
    
    std::cout << "Stopping ContextEngine driver..." << std::endl;
    driver.Stop();
    std::cout << "✓ Driver stopped" << std::endl;
    
    std::cout << "\nQuick start tutorial completed successfully!" << std::endl;
    std::cout << "\nKey takeaways:" << std::endl;
    std::cout << "1. ContextEngine driver provides high-level API for context operations" << std::endl;
    std::cout << "2. Supports both individual and batch context ingestion" << std::endl;
    std::cout << "3. Enables relationship queries and context graph traversal" << std::endl;
    std::cout << "4. Provides streaming ingestion with backpressure handling" << std::endl;
    std::cout << "5. Includes comprehensive statistics and error handling" << std::endl;
    
    return 0;
}
