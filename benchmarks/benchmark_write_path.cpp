#include "nscfstore/write_path.h"
#include <chrono>
#include <thread>
#include <vector>
#include <iostream>
#include <iomanip>

namespace nscfstore {

class WritePathBenchmark {
public:
    WritePathBenchmark() {
        std::filesystem::create_directories("./benchmark_data");
        memory_pool_ = std::make_unique<ShardMemoryPool>(0, 2ULL * 1024 * 1024 * 1024); // 2GB
    }
    
    ~WritePathBenchmark() {
        memory_pool_.reset();
        std::filesystem::remove_all("./benchmark_data");
    }
    
    void run_all_benchmarks() {
        std::cout << "=== Write Path Performance Benchmarks ===" << std::endl;
        std::cout << std::fixed << std::setprecision(2);
        
        benchmark_single_writes();
        benchmark_batch_writes();
        benchmark_concurrent_writes();
        benchmark_wal_performance();
        benchmark_memtable_performance();
        benchmark_flush_performance();
        
        std::cout << "=== Benchmarks Complete ===" << std::endl;
    }
    
private:
    std::unique_ptr<ShardMemoryPool> memory_pool_;
    
    void benchmark_single_writes() {
        std::cout << "\n--- Single Write Performance ---" << std::endl;
        
        WritePathCoordinator coordinator("./benchmark_data/wal_single", memory_pool_.get());
        ASSERT_TRUE(coordinator.start());
        
        const int num_writes = 100000;
        std::vector<double> latencies;
        latencies.reserve(num_writes);
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < num_writes; ++i) {
            auto write_start = std::chrono::high_resolution_clock::now();
            
            Key key = "single_key_" + std::to_string(i);
            Value value = "single_value_" + std::to_string(i);
            coordinator.put(key, value);
            
            auto write_end = std::chrono::high_resolution_clock::now();
            auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                write_end - write_start).count();
            latencies.push_back(latency);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        // Calculate statistics
        std::sort(latencies.begin(), latencies.end());
        double avg_latency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
        double p50_latency = latencies[latencies.size() * 0.5];
        double p95_latency = latencies[latencies.size() * 0.95];
        double p99_latency = latencies[latencies.size() * 0.99];
        
        double throughput = static_cast<double>(num_writes) / total_time * 1000;
        
        std::cout << "Writes: " << num_writes << std::endl;
        std::cout << "Total Time: " << total_time << " ms" << std::endl;
        std::cout << "Throughput: " << throughput << " writes/sec" << std::endl;
        std::cout << "Avg Latency: " << avg_latency << " μs" << std::endl;
        std::cout << "P50 Latency: " << p50_latency << " μs" << std::endl;
        std::cout << "P95 Latency: " << p95_latency << " μs" << std::endl;
        std::cout << "P99 Latency: " << p99_latency << " μs" << std::endl;
        
        auto stats = coordinator.get_stats();
        std::cout << "WAL Write Latency: " << stats.wal_write_latency_us << " μs" << std::endl;
        
        coordinator.stop();
    }
    
    void benchmark_batch_writes() {
        std::cout << "\n--- Batch Write Performance ---" << std::endl;
        
        WritePathCoordinator coordinator("./benchmark_data/wal_batch", memory_pool_.get());
        ASSERT_TRUE(coordinator.start());
        
        const int batch_size = 1000;
        const int num_batches = 100;
        const int total_writes = batch_size * num_batches;
        
        std::vector<double> batch_latencies;
        batch_latencies.reserve(num_batches);
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int batch = 0; batch < num_batches; ++batch) {
            auto batch_start = std::chrono::high_resolution_clock::now();
            
            std::vector<std::pair<Key, Value>> entries;
            for (int i = 0; i < batch_size; ++i) {
                int global_i = batch * batch_size + i;
                entries.emplace_back("batch_key_" + std::to_string(global_i),
                                    "batch_value_" + std::to_string(global_i));
            }
            
            coordinator.put_batch(entries);
            
            auto batch_end = std::chrono::high_resolution_clock::now();
            auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                batch_end - batch_start).count();
            batch_latencies.push_back(latency);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        // Calculate statistics
        std::sort(batch_latencies.begin(), batch_latencies.end());
        double avg_batch_latency = std::accumulate(batch_latencies.begin(), batch_latencies.end(), 0.0) / batch_latencies.size();
        double p95_batch_latency = batch_latencies[batch_latencies.size() * 0.95];
        
        double throughput = static_cast<double>(total_writes) / total_time * 1000;
        double avg_per_write = avg_batch_latency / batch_size;
        
        std::cout << "Total Writes: " << total_writes << std::endl;
        std::cout << "Batch Size: " << batch_size << std::endl;
        std::cout << "Total Time: " << total_time << " ms" << std::endl;
        std::cout << "Throughput: " << throughput << " writes/sec" << std::endl;
        std::cout << "Avg Batch Latency: " << avg_batch_latency << " μs" << std::endl;
        std::cout << "P95 Batch Latency: " << p95_batch_latency << " μs" << std::endl;
        std::cout << "Avg Per-Write Latency: " << avg_per_write << " μs" << std::endl;
        
        coordinator.stop();
    }
    
    void benchmark_concurrent_writes() {
        std::cout << "\n--- Concurrent Write Performance ---" << std::endl;
        
        const int num_threads = 8;
        const int writes_per_thread = 10000;
        const int total_writes = num_threads * writes_per_thread;
        
        WritePathCoordinator coordinator("./benchmark_data/wal_concurrent", memory_pool_.get());
        ASSERT_TRUE(coordinator.start());
        
        std::vector<std::thread> threads;
        std::vector<std::vector<double>> thread_latencies(num_threads);
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < num_threads; ++t) {
            thread_latencies[t].reserve(writes_per_thread);
            threads.emplace_back([&, t]() {
                for (int i = 0; i < writes_per_thread; ++i) {
                    auto write_start = std::chrono::high_resolution_clock::now();
                    
                    Key key = "concurrent_key_" + std::to_string(t) + "_" + std::to_string(i);
                    Value value = "concurrent_value_" + std::to_string(t) + "_" + std::to_string(i);
                    coordinator.put(key, value);
                    
                    auto write_end = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                        write_end - write_start).count();
                    thread_latencies[t].push_back(latency);
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        // Aggregate latency statistics across all threads
        std::vector<double> all_latencies;
        for (const auto& latencies : thread_latencies) {
            all_latencies.insert(all_latencies.end(), latencies.begin(), latencies.end());
        }
        
        std::sort(all_latencies.begin(), all_latencies.end());
        double avg_latency = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0) / all_latencies.size();
        double p95_latency = all_latencies[all_latencies.size() * 0.95];
        double p99_latency = all_latencies[all_latencies.size() * 0.99];
        
        double throughput = static_cast<double>(total_writes) / total_time * 1000;
        
        std::cout << "Threads: " << num_threads << std::endl;
        std::cout << "Writes per Thread: " << writes_per_thread << std::endl;
        std::cout << "Total Writes: " << total_writes << std::endl;
        std::cout << "Total Time: " << total_time << " ms" << std::endl;
        std::cout << "Throughput: " << throughput << " writes/sec" << std::endl;
        std::cout << "Avg Latency: " << avg_latency << " μs" << std::endl;
        std::cout << "P95 Latency: " << p95_latency << " μs" << std::endl;
        std::cout << "P99 Latency: " << p99_latency << " μs" << std::endl;
        
        coordinator.stop();
    }
    
    void benchmark_wal_performance() {
        std::cout << "\n--- WAL Direct I/O Performance ---" << std::endl;
        
        DirectIOWAL wal("./benchmark_data/wal_direct", 64 * 1024 * 1024, 256 * 1024 * 1024);
        ASSERT_TRUE(wal.open());
        
        const int num_records = 100000;
        const size_t record_size = 1024; // 1KB records
        
        std::vector<std::string> test_records(num_records);
        for (int i = 0; i < num_records; ++i) {
            test_records[i] = std::string(record_size, 'x' + (i % 26));
        }
        
        std::vector<double> write_latencies;
        write_latencies.reserve(num_records);
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < num_records; ++i) {
            auto write_start = std::chrono::high_resolution_clock::now();
            
            wal.write_record(test_records[i].data(), test_records[i].size(), i + 1);
            
            auto write_end = std::chrono::high_resolution_clock::now();
            auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                write_end - write_start).count();
            write_latencies.push_back(latency);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        // Test flush performance
        auto flush_start = std::chrono::high_resolution_clock::now();
        wal.flush();
        auto flush_end = std::chrono::high_resolution_clock::now();
        auto flush_latency = std::chrono::duration_cast<std::chrono::microseconds>(
            flush_end - flush_start).count();
        
        // Test sync performance
        auto sync_start = std::chrono::high_resolution_clock::now();
        wal.sync();
        auto sync_end = std::chrono::high_resolution_clock::now();
        auto sync_latency = std::chrono::duration_cast<std::chrono::microseconds>(
            sync_end - sync_start).count();
        
        // Calculate statistics
        std::sort(write_latencies.begin(), write_latencies.end());
        double avg_write_latency = std::accumulate(write_latencies.begin(), write_latencies.end(), 0.0) / write_latencies.size();
        double p95_write_latency = write_latencies[write_latencies.size() * 0.95];
        
        double throughput = static_cast<double>(num_records * record_size) / total_time / 1024 / 1024 * 1000; // MB/sec
        
        std::cout << "Records: " << num_records << std::endl;
        std::cout << "Record Size: " << record_size << " bytes" << std::endl;
        std::cout << "Total Data: " << (num_records * record_size / 1024 / 1024) << " MB" << std::endl;
        std::cout << "Write Throughput: " << throughput << " MB/sec" << std::endl;
        std::cout << "Avg Write Latency: " << avg_write_latency << " μs" << std::endl;
        std::cout << "P95 Write Latency: " << p95_write_latency << " μs" << std::endl;
        std::cout << "Flush Latency: " << flush_latency << " μs" << std::endl;
        std::cout << "Sync Latency: " << sync_latency << " μs" << std::endl;
        
        auto stats = wal.get_stats();
        std::cout << "WAL Statistics Avg Latency: " << stats.avg_write_latency_us << " μs" << std::endl;
        
        wal.close();
    }
    
    void benchmark_memtable_performance() {
        std::cout << "\n--- Lock-Free Memtable Performance ---" << std::endl;
        
        LockFreeMemtable<Key, Value> memtable(memory_pool_.get(), 128 * 1024 * 1024);
        
        const int num_operations = 100000;
        const int read_ratio = 70; // 70% reads, 30% writes
        
        std::vector<std::pair<Key, Value>> test_data;
        for (int i = 0; i < num_operations; ++i) {
            test_data.emplace_back("memtable_key_" + std::to_string(i),
                                 "memtable_value_" + std::to_string(i));
        }
        
        // Populate with initial data
        auto populate_start = std::chrono::high_resolution_clock::now();
        for (const auto& [key, value] : test_data) {
            memtable.put(key, value);
        }
        auto populate_end = std::chrono::high_resolution_clock::now();
        auto populate_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            populate_end - populate_start).count();
        
        // Benchmark mixed operations
        std::vector<double> operation_latencies;
        operation_latencies.reserve(num_operations);
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 99);
        
        for (int i = 0; i < num_operations; ++i) {
            auto op_start = std::chrono::high_resolution_clock::now();
            
            if (dis(gen) < read_ratio) {
                // Read operation
                Value value;
                int key_index = dis(gen) % test_data.size();
                memtable.get(test_data[key_index].first, value);
            } else {
                // Write operation
                Key key = "new_key_" + std::to_string(i);
                Value value = "new_value_" + std::to_string(i);
                memtable.put(key, value);
            }
            
            auto op_end = std::chrono::high_resolution_clock::now();
            auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                op_end - op_start).count();
            operation_latencies.push_back(latency);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        // Calculate statistics
        std::sort(operation_latencies.begin(), operation_latencies.end());
        double avg_latency = std::accumulate(operation_latencies.begin(), operation_latencies.end(), 0.0) / operation_latencies.size();
        double p95_latency = operation_latencies[operation_latencies.size() * 0.95];
        double p99_latency = operation_latencies[operation_latencies.size() * 0.99];
        
        double throughput = static_cast<double>(num_operations) / total_time * 1000;
        
        std::cout << "Operations: " << num_operations << std::endl;
        std::cout << "Read Ratio: " << read_ratio << "%" << std::endl;
        std::cout << "Populate Time: " << populate_time << " ms" << std::endl;
        std::cout << "Mixed Operations Time: " << total_time << " ms" << std::endl;
        std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
        std::cout << "Avg Latency: " << avg_latency << " ns" << std::endl;
        std::cout << "P95 Latency: " << p95_latency << " ns" << std::endl;
        std::cout << "P99 Latency: " << p99_latency << " ns" << std::endl;
        std::cout << "Final Entry Count: " << memtable.entry_count() << std::endl;
        std::cout << "Final Size: " << memtable.size_bytes() / 1024 << " KB" << std::endl;
    }
    
    void benchmark_flush_performance() {
        std::cout << "\n--- Memtable Flush Performance ---" << std::endl;
        
        WritePathCoordinator coordinator("./benchmark_data/wal_flush", memory_pool_.get(), 16 * 1024 * 1024); // 16MB threshold
        ASSERT_TRUE(coordinator.start());
        
        // Fill memtable to trigger flush
        const int num_entries = 50000;
        const size_t value_size = 512; // 512 bytes per value
        
        std::vector<std::pair<Key, Value>> entries;
        for (int i = 0; i < num_entries; ++i) {
            Value large_value(value_size, 'x' + (i % 26));
            entries.emplace_back("flush_key_" + std::to_string(i), large_value);
        }
        
        auto write_start = std::chrono::high_resolution_clock::now();
        coordinator.put_batch(entries);
        auto write_end = std::chrono::high_resolution_clock::now();
        auto write_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            write_end - write_start).count();
        
        // Wait for flush to complete
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        
        auto flush_start = std::chrono::high_resolution_clock::now();
        while (coordinator.flush_in_progress()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        auto flush_end = std::chrono::high_resolution_clock::now();
        auto flush_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            flush_end - flush_start).count();
        
        auto stats = coordinator.get_stats();
        
        std::cout << "Entries: " << num_entries << std::endl;
        std::cout << "Value Size: " << value_size << " bytes" << std::endl;
        std::cout << "Total Data: " << (num_entries * value_size / 1024 / 1024) << " MB" << std::endl;
        std::cout << "Write Time: " << write_time << " ms" << std::endl;
        std::cout << "Flush Time: " << flush_time << " ms" << std::endl;
        std::cout << "Total Flushes: " << stats.memtable_flushes << std::endl;
        std::cout << "Write Throughput: " << (num_entries * value_size / 1024.0 / 1024.0) / (write_time / 1000.0) << " MB/sec" << std::endl;
        
        if (flush_time > 0) {
            std::cout << "Flush Throughput: " << (num_entries * value_size / 1024.0 / 1024.0) / (flush_time / 1000.0) << " MB/sec" << std::endl;
        }
        
        coordinator.stop();
    }
};

} // namespace nscfstore

int main() {
    nscfstore::WritePathBenchmark benchmark;
    benchmark.run_all_benchmarks();
    return 0;
}
