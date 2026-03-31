# LSMStore Technical Specification

## 1. System Architecture

### 1.1 Shared-Nothing, Shard-Per-Core Design

LSMStore implements a shared-nothing architecture where each CPU core operates independently as a shard, eliminating lock contention and enabling linear scalability.

#### Core Principles:
- **One Thread Per Core**: Each CPU core runs a dedicated thread with its own execution loop
- **No Shared State**: Threads communicate only via explicit message passing
- **Memory Isolation**: Each shard owns its own memory pools and data structures
- **Deterministic Performance**: No cross-shard contention ensures predictable latencies

### 1.2 Threading Model

```
CPU Core 0    CPU Core 1    CPU Core 2    CPU Core N
    |             |             |             |
Shard 0        Shard 1        Shard 2        Shard N
    |             |             |             |
Memtable       Memtable       Memtable       Memtable
WAL            WAL            WAL            WAL
SSTables       SSTables       SSTables       SSTables
Memory Pool    Memory Pool    Memory Pool    Memory Pool
```

#### Thread Responsibilities:
- **Event Loop**: Process I/O events, timers, and inter-shard messages
- **Compaction**: Local SSTable compaction and maintenance
- **Request Handling**: Process reads/writes for keys mapped to this shard
- **Memory Management**: Manage local memory pools and garbage collection

### 1.3 Memory Management Architecture

#### 1.3.1 Per-Shard Memory Pools

Each shard maintains separate memory pools for different object types:

```cpp
class ShardMemoryPool {
    ArenaPool* memtable_arena;      // Memtable entries
    ArenaPool* wal_buffer_pool;     // WAL write buffers
    BlockPool* sstable_blocks;      // SSTable data blocks
    ObjectPool<Request>* req_pool;  // Request objects
    ObjectPool<Response>* resp_pool; // Response objects
};
```

#### 1.3.2 Memory Allocation Strategy

- **Pre-allocation**: All pools are sized based on system memory and shard count
- **Lock-free Allocation**: Each pool uses lock-free atomic operations
- **NUMA Awareness**: Memory allocated on the local NUMA node for each shard
- **Zero-copy**: Data structures designed to minimize memory copies

#### 1.3.3 Memory Reclamation

- **Epoch-based Reclamation**: Safe memory reclamation without stop-the-world
- **Reference Counting**: For shared objects across shards
- **Lazy Freeing**: Memory returned to pools when shards are idle

## 2. Core Components

### 2.1 Memtable - Lock-free SkipList

#### 2.1.1 Data Structure

```cpp
template<typename Key, typename Value>
class LockFreeSkipList {
    struct Node {
        Key key;
        Value value;
        std::atomic<Node*> next[MAX_LEVEL];
        std::atomic<int> marked;  // Delete marking
        int level;
    };
    
    std::atomic<Node*> head;
    std::atomic<int> max_level;
    ShardMemoryPool* memory_pool;
};
```

#### 2.1.2 Operations

- **Insert**: O(log N) expected, lock-free
- **Search**: O(log N) expected, wait-free
- **Delete**: Logical deletion with lazy physical removal
- **Range Scan**: Efficient iterator-based traversal

#### 2.1.3 Memory Management

- **Node Allocation**: From shard's memtable arena
- **SMR Integration**: Nodes reclaimed using epoch-based reclamation
- **Compaction Trigger**: Size-based or time-based thresholds

### 2.2 Write-Ahead Log (WAL)

#### 2.2.1 Architecture

```cpp
class WAL {
    struct LogEntry {
        uint64_t sequence_number;
        OperationType op_type;
        Key key;
        Value value;
        uint32_t checksum;
    };
    
    RingBuffer<LogEntry> buffer;
    AsyncFileWriter writer;
    std::atomic<uint64_t> sequence;
};
```

#### 2.2.2 Write Path

1. **Sequence Assignment**: Atomic counter for global ordering
2. **Buffer Write**: Lock-free write to per-shard ring buffer
3. **Async Flush**: io_uring-based async file writes
4. **Durability Guarantee**: Ack after fsync completion

#### 2.2.3 Recovery

- **Crash Recovery**: Scan WAL files to rebuild memtables
- **Checkpointing**: Periodic memtable snapshots to reduce recovery time
- **Log Truncation**: Remove obsolete entries after compaction

### 2.3 SSTable Manager

#### 2.3.1 SSTable Format

```
[SSTable Header]
[Data Block 1][Data Block 2]...[Data Block N]
[Index Block]
[Filter Block]
[Footer]
```

#### 2.3.2 Data Blocks

- **Compression**: LZ4 or ZSTD for space efficiency
- **Block Size**: Configurable (default 4KB)
- **Encoding**: Delta encoding for keys, varint encoding

#### 2.3.3 Index Structure

```cpp
class SSTableIndex {
    struct IndexEntry {
        Key first_key;
        uint64_t offset;
        uint64_t size;
    };
    
    std::vector<IndexEntry> entries;
    BloomFilter filter;
};
```

#### 2.3.4 Compaction Strategy

- **Leveled Compaction**: Similar to LevelDB/RocksDB
- **Size-tiered Compaction**: Alternative for write-heavy workloads
- **Parallel Compaction**: Each shard compacts its own SSTables independently

## 3. Network Stack

### 3.1 io_uring Integration

#### 3.1.1 Architecture

```cpp
class IoUringNetworkStack {
    io_uring ring;
    std::vector<Connection> connections;
    MessageQueue inbound_queue;
    MessageQueue outbound_queue;
};
```

#### 3.1.2 Connection Handling

- **Accept Loop**: Async accept using io_uring
- **Read Path**: Async reads with zero-copy buffers
- **Write Path**: Batched writes with vectored I/O
- **Connection Pool**: Per-shard connection management

#### 3.1.3 Protocol

```
[Header: 8 bytes][Payload: variable]

Header Format:
- Magic: 2 bytes (0xCAFE)
- Version: 1 byte
- Op Type: 1 byte (GET/PUT/DELETE/SCAN)
- Length: 4 bytes (payload size)
```

### 3.2 Request Processing

#### 3.2.1 Request Routing

```cpp
class RequestRouter {
    uint64_t hash_key(const Key& key);
    uint32_t get_shard_id(const Key& key);
    void route_request(Request* req);
};
```

#### 3.2.2 Request Pipeline

1. **Receive**: Async read from network
2. **Parse**: Deserialize request
3. **Route**: Determine target shard based on key hash
4. **Execute**: Process on target shard
5. **Respond**: Async write back to client

## 4. Performance Optimizations

### 4.1 Cache Efficiency

- **Data Locality**: Related data stored in same shard
- **Prefetching**: Hardware prefetch for sequential accesses
- **Cache Line Alignment**: All structures aligned to cache lines
- **False Sharing Prevention**: Padding between hot variables

### 4.2 CPU Optimizations

- **Branch Prediction**: Minimize branches in hot paths
- **SIMD Instructions**: Vectorized operations where applicable
- **CPU Affinity**: Pin threads to specific cores
- **NUMA Optimization**: Memory allocated on local NUMA nodes

### 4.3 I/O Optimizations

- **Batching**: Group multiple operations together
- **Async I/O**: Non-blocking operations throughout
- **Direct I/O**: Bypass page cache for large files
- **Write Combining**: Merge small writes into larger ones

## 5. Configuration and Tuning

### 5.1 System Parameters

```yaml
system:
  num_shards: 0  # 0 = auto-detect CPU cores
  memory_per_shard: "2GB"
  wal_buffer_size: "64MB"
  memtable_threshold: "128MB"

compaction:
  strategy: "leveled"  # or "size_tiered"
  level_multiplier: 10
  max_levels: 7

network:
  listen_port: 8080
  max_connections: 10000
  recv_buffer_size: "64KB"
  send_buffer_size: "64KB"
```

### 5.2 Performance Tuning

- **Shard Count**: Typically equals CPU core count
- **Memory Allocation**: 70% for data, 30% for buffers
- **Compaction Throttling**: Balance between write and read performance
- **Network Buffers**: Size based on expected request patterns

## 6. Monitoring and Observability

### 6.1 Metrics

- **Throughput**: Operations per second per shard
- **Latency**: P50, P95, P99 latencies
- **Memory Usage**: Per-shard memory consumption
- **Disk I/O**: Read/write bandwidth and IOPS
- **Compaction**: Compaction rate and pause times

### 6.2 Tracing

- **Request Flow**: End-to-end request tracing
- **System Events**: Compaction, recovery, and error events
- **Performance Analysis**: Hot path identification

## 7. Reliability and Fault Tolerance

### 7.1 Data Durability

- **WAL Guarantee**: All writes flushed to disk before ack
- **Replication**: Optional multi-replica support
- **Checksums**: End-to-end data integrity verification

### 7.2 Error Handling

- **Partial Failures**: Graceful degradation on component failures
- **Recovery**: Automatic recovery from crashes
- **Consistency**: Strong consistency guarantees within single node

## 8. Future Extensions

### 8.1 Distributed Mode

- **Shard Replication**: Multi-node shard replication
- **Consensus**: Raft-based consensus for distributed operations
- **Load Balancing**: Automatic shard rebalancing

### 8.2 Advanced Features

- **Transactions**: Multi-key transaction support
- **Secondary Indexes**: Efficient range queries
- **Time Series**: Optimized for time-series data
- **Analytics**: Built-in aggregation and analytics capabilities
