#include <gtest/gtest.h>
#include "nscfstore/write_path.h"
#include <chrono>
#include <thread>
#include <vector>

namespace nscfstore {

class WritePathTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory
        std::filesystem::create_directories("./test_data");
        
        // Initialize memory pool
        memory_pool_ = std::make_unique<ShardMemoryPool>(0, 1024 * 1024 * 1024); // 1GB
        
        // Initialize write path coordinator
        coordinator_ = std::make_unique<WritePathCoordinator>(
            "./test_data/wal", memory_pool_.get(), 64 * 1024 * 1024); // 64MB threshold
    }
    
    void TearDown() override {
        coordinator_.reset();
        memory_pool_.reset();
        
        // Cleanup test directory
        std::filesystem::remove_all("./test_data");
    }
    
    std::unique_ptr<ShardMemoryPool> memory_pool_;
    std::unique_ptr<WritePathCoordinator> coordinator_;
};

TEST_F(WritePathTest, BasicPutOperation) {
    ASSERT_TRUE(coordinator_->start());
    
    Key key = "test_key";
    Value value = "test_value";
    
    // Test basic put
    EXPECT_TRUE(coordinator_->put(key, value));
    
    // Check statistics
    auto stats = coordinator_->get_stats();
    EXPECT_EQ(stats.total_puts, 1);
    EXPECT_GT(stats.total_bytes_written, 0);
    EXPECT_GT(stats.avg_put_latency_us, 0);
    EXPECT_GT(stats.wal_writes, 0);
    
    coordinator_->stop();
}

TEST_F(WritePathTest, BatchPutOperation) {
    ASSERT_TRUE(coordinator_->start());
    
    std::vector<std::pair<Key, Value>> entries;
    for (int i = 0; i < 100; ++i) {
        entries.emplace_back("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    // Test batch put
    EXPECT_TRUE(coordinator_->put_batch(entries));
    
    // Check statistics
    auto stats = coordinator_->get_stats();
    EXPECT_EQ(stats.total_puts, 100);
    EXPECT_GT(stats.total_bytes_written, 0);
    EXPECT_GT(stats.wal_writes, 0);
    
    coordinator_->stop();
}

TEST_F(WritePathTest, MemtableFlush) {
    ASSERT_TRUE(coordinator_->start());
    
    // Fill memtable to trigger flush (using small threshold for testing)
    std::vector<std::pair<Key, Value>> entries;
    for (int i = 0; i < 1000; ++i) {
        // Create large values to fill memtable quickly
        Value large_value(1024, 'x' + (i % 26));
        entries.emplace_back("large_key_" + std::to_string(i), large_value);
    }
    
    EXPECT_TRUE(coordinator_->put_batch(entries));
    
    // Wait a bit for flush to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Check if flush was triggered
    auto stats = coordinator_->get_stats();
    EXPECT_GT(stats.memtable_flushes, 0);
    
    coordinator_->stop();
}

TEST_F(WritePathTest, DirectIOWAL) {
    DirectIOWAL wal("./test_data/direct_wal", 1024 * 1024, 16 * 1024 * 1024);
    
    ASSERT_TRUE(wal.open());
    
    // Test single record write
    std::string test_data = "Hello, Direct I/O WAL!";
    uint64_t sequence = 1;
    EXPECT_TRUE(wal.write_record(test_data.data(), test_data.size(), sequence));
    
    // Test batch write
    std::vector<std::pair<const void*, size_t>> batch_data;
    std::vector<uint64_t> sequences;
    
    for (int i = 0; i < 10; ++i) {
        std::string data = "Batch record " + std::to_string(i);
        batch_data.emplace_back(data.data(), data.size());
        sequences.push_back(sequence + i + 1);
    }
    
    EXPECT_TRUE(wal.write_batch(batch_data, sequences));
    
    // Test flush and sync
    EXPECT_TRUE(wal.flush());
    EXPECT_TRUE(wal.sync());
    
    // Check statistics
    auto stats = wal.get_stats();
    EXPECT_EQ(stats.total_writes, 11); // 1 single + 10 batch
    EXPECT_EQ(stats.total_flushes, 1);
    EXPECT_EQ(stats.total_syncs, 1);
    EXPECT_GT(stats.total_bytes_written, 0);
    
    wal.close();
}

TEST_F(WritePathTest, LockFreeMemtable) {
    LockFreeMemtable<Key, Value> memtable(memory_pool_.get(), 1024 * 1024);
    
    // Test basic operations
    EXPECT_TRUE(memtable.put("key1", "value1"));
    EXPECT_TRUE(memtable.put("key2", "value2"));
    EXPECT_TRUE(memtable.put("key3", "value3"));
    
    // Test get operations
    Value value;
    EXPECT_TRUE(memtable.get("key1", value));
    EXPECT_EQ(value, "value1");
    
    EXPECT_TRUE(memtable.get("key2", value));
    EXPECT_EQ(value, "value2");
    
    EXPECT_FALSE(memtable.get("nonexistent_key", value));
    
    // Test remove
    EXPECT_TRUE(memtable.remove("key2"));
    EXPECT_FALSE(memtable.get("key2", value));
    
    // Test iterator
    auto entries = memtable.get_all_entries();
    EXPECT_EQ(entries.size(), 2); // key2 was removed
    
    // Check if entries are sorted
    EXPECT_TRUE(std::is_sorted(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; }));
    
    // Check statistics
    EXPECT_EQ(memtable.entry_count(), 2);
    EXPECT_GT(memtable.size_bytes(), 0);
}

TEST_F(WritePathTest, ConcurrentMemtableAccess) {
    LockFreeMemtable<Key, Value> memtable(memory_pool_.get(), 10 * 1024 * 1024);
    
    const int num_threads = 4;
    const int operations_per_thread = 1000;
    std::vector<std::thread> threads;
    
    // Start concurrent writes
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&memtable, t, operations_per_thread]() {
            for (int i = 0; i < operations_per_thread; ++i) {
                Key key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                Value value = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);
                memtable.put(key, value);
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify all entries were written
    auto entries = memtable.get_all_entries();
    EXPECT_EQ(entries.size(), num_threads * operations_per_thread);
    
    // Verify some random entries
    Value value;
    EXPECT_TRUE(memtable.get("thread_0_key_0", value));
    EXPECT_EQ(value, "thread_0_value_0");
    
    EXPECT_TRUE(memtable.get("thread_3_key_999", value));
    EXPECT_EQ(value, "thread_3_value_999");
}

TEST_F(WritePathTest, SSTableLevel0Builder) {
    // Create test entries
    std::vector<std::pair<Key, Value>> entries;
    for (int i = 0; i < 100; ++i) {
        entries.emplace_back("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    // Build SSTable
    SSTableLevel0Builder builder("./test_data/test_sstable.sst", memory_pool_.get());
    EXPECT_TRUE(builder.build_from_entries(entries));
    
    // Verify file was created
    EXPECT_GT(builder.get_file_size(), 0);
    EXPECT_EQ(builder.get_entry_count(), 100);
    
    // Verify file exists
    EXPECT_TRUE(std::filesystem::exists("./test_data/test_sstable.sst"));
}

TEST_F(WritePathTest, PerformanceMetrics) {
    ASSERT_TRUE(coordinator_->start());
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Perform many writes
    const int num_writes = 10000;
    for (int i = 0; i < num_writes; ++i) {
        Key key = "perf_key_" + std::to_string(i);
        Value value = "perf_value_" + std::to_string(i);
        coordinator_->put(key, value);
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    // Check performance metrics
    auto stats = coordinator_->get_stats();
    EXPECT_EQ(stats.total_puts, num_writes);
    EXPECT_GT(stats.total_bytes_written, 0);
    EXPECT_GT(stats.avg_put_latency_us, 0);
    EXPECT_LT(stats.avg_put_latency_us, 1000); // Should be less than 1ms on average
    
    // Check throughput
    double throughput = static_cast<double>(num_writes) / duration.count() * 1000; // ops/sec
    EXPECT_GT(throughput, 1000); // Should handle at least 1000 ops/sec
    
    std::cout << "Write throughput: " << throughput << " ops/sec" << std::endl;
    std::cout << "Average put latency: " << stats.avg_put_latency_us << " μs" << std::endl;
    std::cout << "WAL write latency: " << stats.wal_write_latency_us << " μs" << std::endl;
    
    coordinator_->stop();
}

TEST_F(WritePathTest, WALRecovery) {
    DirectIOWAL wal("./test_data/recovery_wal", 1024 * 1024, 16 * 1024 * 1024);
    
    ASSERT_TRUE(wal.open());
    
    // Write some records
    std::vector<std::string> test_records = {
        "record1", "record2", "record3", "record4", "record5"
    };
    
    for (size_t i = 0; i < test_records.size(); ++i) {
        EXPECT_TRUE(wal.write_record(test_records[i].data(), test_records[i].size(), i + 1));
    }
    
    EXPECT_TRUE(wal.flush());
    EXPECT_TRUE(wal.sync());
    
    uint64_t last_sequence = wal.get_last_sequence();
    EXPECT_EQ(last_sequence, test_records.size());
    
    wal.close();
    
    // Reopen WAL and verify recovery
    ASSERT_TRUE(wal.open());
    
    // In a real implementation, we would read back the records
    // For now, just verify the sequence number is preserved
    EXPECT_EQ(wal.get_last_sequence(), last_sequence);
    
    wal.close();
}

TEST_F(WritePathTest, MemoryUsage) {
    ASSERT_TRUE(coordinator_->start());
    
    // Monitor memory usage during writes
    auto initial_stats = coordinator_->get_stats();
    
    // Write data that will fill memtable
    std::vector<std::pair<Key, Value>> large_entries;
    for (int i = 0; i < 1000; ++i) {
        Value large_value(10 * 1024, 'x'); // 10KB values
        large_entries.emplace_back("large_key_" + std::to_string(i), large_value);
    }
    
    EXPECT_TRUE(coordinator_->put_batch(large_entries));
    
    // Wait for potential flush
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    auto final_stats = coordinator_->get_stats();
    
    // Verify memory usage tracking
    EXPECT_GT(final_stats.total_bytes_written, initial_stats.total_bytes_written);
    
    // Check if flush was triggered due to memory pressure
    if (final_stats.memtable_flushes > initial_stats.memtable_flushes) {
        std::cout << "Memtable flush triggered due to memory pressure" << std::endl;
    }
    
    coordinator_->stop();
}

} // namespace nscfstore
