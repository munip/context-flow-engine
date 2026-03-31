# LSMStore - High-Throughput LSM-based Storage Engine

A high-performance, shared-nothing LSM-tree storage engine built in C++ with a shard-per-core architecture inspired by ScyllaDB/Seastar for powering context engines.

## Architecture Overview

- **Shared-Nothing Design**: One thread per CPU core with no shared state
- **Memory Management**: Pre-allocated memory pools per shard
- **Core Components**: Lock-free SkipList Memtable, WAL, and SSTable manager
- **Network Stack**: Asynchronous I/O using io_uring for high concurrency

## Building

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Running

```bash
./nscfstore --config config.yaml
```

## Performance Targets

- **Throughput**: >1M ops/sec on modern hardware
- **Latency**: <1ms p99 for reads/writes
- **Scalability**: Linear scaling with CPU cores

## Project Structure

```
├── include/           # Header files
├── src/              # Source implementation
├── tests/            # Unit tests
├── benchmarks/       # Performance benchmarks
├── config/           # Configuration files
└── docs/             # Documentation
```
