# ContextEngine Quick Start Guide

This guide will help you get started with the ContextEngine gRPC API layer, demonstrating how to ingest context data, query relationships, and traverse context graphs.

## Prerequisites

### System Requirements
- Linux (Ubuntu 20.04+ recommended)
- C++20 compatible compiler
- gRPC and Protocol Buffers
- CMake 3.20+
- 16GB+ RAM recommended for production

### Dependencies Installation

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libxxhash-dev \
    pkg-config

# Install gRPC (if not available via package manager)
git clone --recurse-submodules -b v1.54.0 https://github.com/grpc/grpc
cd grpc
mkdir build && cd build
cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF
make -j$(nproc)
sudo make install
sudo ldconfig
```

## Building ContextEngine

```bash
# Clone the repository
git clone <repository-url>
cd context-flow-engine

# Create build directory
mkdir build && cd build

# Configure and build
cmake ..
make -j$(nproc)

# Run tests (optional)
make test

# Run benchmarks (optional)
make benchmark
```

## Starting the Server

```bash
# Start the ContextEngine server
./nscfstore --config config/default.yaml

# Server will start on default port 50051
# You should see output like:
# "gRPC server started on 0.0.0.0:50051"
# "LSMStore started successfully"
```

## Client Connection

### Basic Client Setup

```cpp
#include "nscfstore/grpc_client.h"

using namespace nscfstore;

// Create driver instance
ContextEngineDriver driver("localhost:50051");

// Configure connection
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
    std::cerr << "Failed to start driver" << std::endl;
    return 1;
}
```

## Core Operations

### 1. Ingesting Context Data

```cpp
// Create user context with attributes and relationships
std::map<std::string, std::string> attributes = {
    {"name", "Alice"},
    {"age", "30"},
    {"department", "Engineering"},
    {"role", "Senior Developer"}
};

std::vector<std::pair<std::string, std::string>> relationships = {
    {"project_alpha", "works_on"},
    {"team_engineering", "member_of"},
    {"bob", "collaborates_with"}
};

// Ingest the context
bool success = driver.IngestContext("user_alice", "user", attributes, relationships);
if (success) {
    std::cout << "Context ingested successfully" << std::endl;
}
```

### 2. Querying Relationships

```cpp
// Get all relationships for an entity
std::vector<std::pair<std::string, std::string>> relationships;
if (driver.GetRelationships("user_alice", relationships)) {
    std::cout << "Found " << relationships.size() << " relationships:" << std::endl;
    for (const auto& [target, type] : relationships) {
        std::cout << "  - " << target << " (" << type << ")" << std::endl;
    }
}

// Query specific relationship type
std::vector<std::pair<std::string, std::string>> work_relationships;
if (driver.GetRelationships("user_alice", work_relationships, "works_on")) {
    std::cout << "Work relationships:" << std::endl;
    for (const auto& [target, type] : work_relationships) {
        std::cout << "  - " << target << std::endl;
    }
}
```

### 3. Context Graph Traversal

```cpp
// Get context graph with depth traversal
std::vector<std::string> connected_entities;
std::vector<std::tuple<std::string, std::string, std::string>> edges;

if (driver.GetContextGraph("user_alice", 2, connected_entities, edges)) {
    std::cout << "Connected entities (" << connected_entities.size() << "):" << std::endl;
    for (const auto& entity : connected_entities) {
        std::cout << "  - " << entity << std::endl;
    }
    
    std::cout << "Relationships (" << edges.size() << "):" << std::endl;
    for (const auto& [source, target, type] : edges) {
        std::cout << "  - " << source << " -> " << target << " (" << type << ")" << std::endl;
    }
}
```

## Advanced Features

### Batch Operations

```cpp
// Prepare batch contexts
std::vector<std::tuple<std::string, std::string, std::map<std::string, std::string>>> batch_contexts;

batch_contexts.emplace_back("user_bob", "user", std::map<std::string, std::string>{
    {"name", "Bob"},
    {"department", "Engineering"},
    {"role", "Developer"}
});

batch_contexts.emplace_back("project_beta", "project", std::map<std::string, std::string>{
    {"name", "Project Beta"},
    {"type", "research"},
    {"status", "planning"}
});

// Batch ingest
if (driver.BatchIngestContext(batch_contexts)) {
    std::cout << "Batch ingestion successful" << std::endl;
}
```

### Streaming Ingestion

```cpp
// Create streaming ingestor for high-throughput scenarios
auto ingestor = std::make_unique<ContextEngineDriver::StreamingIngestor>(&driver);

if (ingestor->Start()) {
    // Add contexts via streaming
    for (int i = 0; i < 1000; ++i) {
        std::string entity_id = "entity_" + std::to_string(i);
        std::map<std::string, std::string> attributes = {
            {"index", std::to_string(i)},
            {"batch", "streaming"}
        };
        
        if (!ingestor->AddContext(entity_id, "stream_context", attributes)) {
            std::cerr << "Failed to add context: " << entity_id << std::endl;
        }
    }
    
    // Get streaming statistics
    auto stats = ingestor->GetStats();
    std::cout << "Streaming stats:" << std::endl;
    std::cout << "  Total: " << stats.total_sent << std::endl;
    std::cout << "  Success: " << stats.total_success << std::endl;
    std::cout << "  Failed: " << stats.total_failed << std::endl;
    std::cout << "  Avg Latency: " << stats.avg_latency_ms << " ms" << std::endl;
    
    ingestor->Stop();
}
```

### Backpressure Handling

The streaming ingestor automatically handles backpressure from the server:

```cpp
// Set up backpressure callback
ingestor->SetResponseCallback([](const context_engine::ContextIngestResponse& response) {
    if (response.has_flow_control()) {
        const auto& fc = response.flow_control();
        if (fc.should_slow_down()) {
            std::cout << "Server signaled to slow down (backlog: " 
                     << fc.current_backlog << ")" << std::endl;
            // Application can implement throttling logic here
        }
    }
});
```

## Configuration

### Server Configuration

Edit `config/default.yaml` to customize server behavior:

```yaml
system:
  num_shards: 0  # Auto-detect CPU cores
  memory_per_shard: "2GB"
  data_dir: "./data"

network:
  listen_port: 8080
  max_connections: 10000
  io_uring_depth: 4096

grpc:
  server_address: "0.0.0.0:50051"
  max_message_size: "64MB"
  timeout_ms: 30000
  enable_reflection: true

backpressure:
  max_queue_depth: 10000
  max_compaction_backlog: 100
  max_memtable_utilization: 0.8
  flow_control_window_size: 1000
```

### Client Configuration

```cpp
ContextEngineDriver::DriverConfig config;
config.server_address = "localhost:50051";
config.connection_retry_interval_ms = 3000;
config.max_connection_retries = 5;
config.operation_timeout_ms = 30000;
config.enable_auto_reconnect = true;
config.enable_metrics = true;
```

## Performance Tuning

### Server-Side Optimizations

1. **Shard Configuration**: Set `num_shards` to match CPU cores
2. **Memory Allocation**: Adjust `memory_per_shard` based on available RAM
3. **Backpressure Thresholds**: Tune based on workload characteristics
4. **Compaction Strategy**: Choose between size-tiered and leveled compaction

### Client-Side Optimizations

1. **Batch Operations**: Use batch ingest for high-throughput scenarios
2. **Streaming**: Use streaming ingestion for continuous data flows
3. **Connection Pooling**: Reuse connections for multiple operations
4. **Timeout Settings**: Adjust based on network latency and operation complexity

## Monitoring and Statistics

### Server Statistics

```cpp
// Get system statistics from server
context_engine::SystemStatsRequest request;
request.set_include_shard_stats(true);
request.set_include_performance_stats(true);

context_engine::SystemStatsResponse response;
if (client->GetSystemStats(request, response)) {
    std::cout << "Server statistics:" << std::endl;
    std::cout << "  Shards: " << response.shard_stats_size() << std::endl;
    std::cout << "  Avg write latency: " << response.performance_stats().avg_write_latency_us() << " μs" << std::endl;
    std::cout << "  Avg read latency: " << response.performance_stats().avg_read_latency_us() << " μs" << std::endl;
}
```

### Client Statistics

```cpp
// Get driver statistics
auto stats = driver.GetStats();
std::cout << "Client statistics:" << std::endl;
std::cout << "  Total operations: " << stats.total_operations << std::endl;
std::cout << "  Success rate: " << (double)stats.successful_operations / stats.total_operations * 100 << "%" << std::endl;
std::cout << "  Average latency: " << stats.avg_latency_ms << " ms" << std::endl;
std::cout << "  Reconnections: " << stats.reconnections << std::endl;
```

## Error Handling

### Common Error Scenarios

```cpp
// Check for connection errors
if (!driver.IsRunning()) {
    auto stats = driver.GetStats();
    std::cerr << "Driver error: " << stats.last_error << std::endl;
}

// Handle operation failures
if (!driver.IngestContext("entity_id", "type", attributes)) {
    auto stats = driver.GetStats();
    std::cerr << "Ingestion failed: " << stats.last_error << std::endl;
    
    // Implement retry logic if needed
    if (stats.reconnections > 0) {
        std::cout << "Connection was reestablished, retrying..." << std::endl;
        // Retry the operation
    }
}
```

## Running the Quick Start Example

```bash
# Build the project
cd build
make -j$(nproc)

# Start the server in one terminal
./nscfstore &

# Run the quick start example in another terminal
./quick_start

# You should see output showing:
# - Driver initialization
# - Context ingestion
# - Relationship queries
# - Context graph traversal
# - Batch operations
# - Streaming ingestion
# - Final statistics
```

## Next Steps

1. **Explore Advanced Features**: Try different relationship types and inference
2. **Performance Testing**: Use the benchmark suite to test throughput
3. **Production Deployment**: Configure for production workloads
4. **Custom Clients**: Build clients in other languages using the protobuf definitions
5. **Monitoring**: Set up comprehensive monitoring and alerting

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check if server is running and accessible
2. **Timeout Errors**: Increase timeout values for large operations
3. **Memory Issues**: Reduce memory allocation or increase system RAM
4. **Performance Issues**: Check system resources and adjust configuration

### Debug Mode

```bash
# Build with debug symbols
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)

# Run with debug logging
./nscfstore --config config/default.yaml --debug
```

For more detailed information, refer to the technical specification and API documentation.
