#pragma once

#include "common.h"
#include "memory_pool.h"
#include <atomic>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace nscfstore {

struct WALHeader {
    uint64_t magic;
    uint32_t version;
    uint64_t sequence_number;
    uint32_t checksum;
};

struct WALRecord {
    uint64_t sequence;
    OperationType op_type;
    uint32_t key_size;
    uint32_t value_size;
    uint32_t checksum;
    // Followed by key data and value data
};

class WALWriter {
public:
    explicit WALWriter(const std::string& file_path, ShardMemoryPool* pool);
    ~WALWriter();
    
    bool write_record(const WALRecord& record, const Key& key, const Value& value);
    bool flush();
    bool sync();
    
    uint64_t current_sequence() const { return sequence_.load(); }
    bool is_open() const { return fd_ >= 0; }
    uint64_t next_sequence() { return sequence_.fetch_add(1) + 1; }
    
private:
    std::string file_path_;
    int fd_;
    ShardMemoryPool* memory_pool_;
    std::atomic<uint64_t> sequence_{0};
    
    // Write buffer
    std::vector<char> write_buffer_;
    std::mutex buffer_mutex_;
    
    uint32_t calculate_checksum(const WALRecord& record, const Key& key, const Value& value);
};

class WALReader {
public:
    explicit WALReader(const std::string& file_path);
    ~WALReader();
    
    bool read_record(WALRecord& record, Key& key, Value& value);
    bool seek_to_sequence(uint64_t sequence);
    
private:
    std::string file_path_;
    int fd_;
    std::vector<char> read_buffer_;
    size_t buffer_offset_;
    
    bool read_buffer(size_t bytes, void* dest);
};

class WAL {
public:
    explicit WAL(uint32_t shard_id, const std::string& data_dir, ShardMemoryPool* pool);
    ~WAL();
    
    bool start();
    void stop();
    
    // Write operations
    bool write_put(const Key& key, const Value& value);
    bool write_delete(const Key& key);
    bool flush();
    
    // Recovery
    bool recover(std::function<void(const Key&, const Value&)> on_put,
                 std::function<void(const Key&)> on_delete);
    
    // Maintenance
    void checkpoint(uint64_t sequence);
    void truncate(uint64_t sequence);
    
    uint64_t last_sequence() const { return last_sequence_.load(); }
    
private:
    uint32_t shard_id_;
    std::string data_dir_;
    ShardMemoryPool* memory_pool_;
    
    std::unique_ptr<WALWriter> writer_;
    std::atomic<bool> running_{false};
    
    std::atomic<uint64_t> last_sequence_{0};
    std::atomic<uint64_t> checkpointed_sequence_{0};
    
    // Background flush thread
    std::thread flush_thread_;
    std::condition_variable flush_cv_;
    std::mutex flush_mutex_;
    std::atomic<bool> flush_pending_{false};
    
    void flush_worker();
    std::string get_wal_file_path(uint64_t segment);
    bool rotate_wal_file();
};

} // namespace nscfstore
