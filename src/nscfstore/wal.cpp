#include "nscfstore/wal.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

namespace nscfstore {

// WALWriter Implementation
WALWriter::WALWriter(const std::string& file_path, ShardMemoryPool* pool) 
    : file_path_(file_path), fd_(-1), memory_pool_(pool) {
    // Open file in constructor
    fd_ = ::open(file_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
}

WALWriter::~WALWriter() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

bool WALWriter::write_record(const WALRecord& record, const Key& key, const Value& value) {
    if (fd_ < 0) return false;
    
    // Write record header
    if (::write(fd_, &record, sizeof(record)) != sizeof(record)) {
        return false;
    }
    
    // Write key
    if (::write(fd_, key.data(), key.size()) != static_cast<ssize_t>(key.size())) {
        return false;
    }
    
    // Write value
    if (::write(fd_, value.data(), value.size()) != static_cast<ssize_t>(value.size())) {
        return false;
    }
    
    return true;
}

uint32_t WALWriter::calculate_checksum(const WALRecord& record, const Key& key, const Value& value) {
    uint32_t checksum = 0;
    checksum ^= record.sequence;
    checksum ^= static_cast<uint32_t>(record.op_type);
    checksum ^= record.key_size;
    checksum ^= record.value_size;
    
    for (char c : key) {
        checksum ^= static_cast<uint32_t>(c);
    }
    
    for (char c : value) {
        checksum ^= static_cast<uint32_t>(c);
    }
    
    return checksum;
}

bool WALWriter::flush() {
    if (fd_ < 0) return false;
    
    // Simple flush - sync to disk
    return ::fsync(fd_) == 0;
}

bool WALWriter::sync() {
    if (fd_ < 0) return false;
    
    return ::fsync(fd_) == 0;
}

// WALReader Implementation
WALReader::WALReader(const std::string& file_path) : file_path_(file_path), fd_(-1), buffer_offset_(0) {
}

WALReader::~WALReader() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

bool WALReader::read_record(WALRecord& record, Key& key, Value& value) {
    if (fd_ < 0) return false;
    
    // Read record header
    if (!read_buffer(sizeof(record), &record)) {
        return false;
    }
    
    // Read key
    key.resize(record.key_size);
    if (!read_buffer(record.key_size, key.data())) {
        return false;
    }
    
    // Read value
    value.resize(record.value_size);
    if (!read_buffer(record.value_size, value.data())) {
        return false;
    }
    
    return true;
}

bool WALReader::seek_to_sequence(uint64_t sequence) {
    // For now, just reopen from beginning
    if (fd_ >= 0) {
        ::close(fd_);
    }
    
    fd_ = ::open(file_path_.c_str(), O_RDONLY);
    if (fd_ < 0) return false;
    
    buffer_offset_ = 0;
    read_buffer_.resize(4096);
    
    // Skip records until we find the target sequence
    WALRecord record;
    Key key;
    Value value;
    
    while (read_record(record, key, value)) {
        if (record.sequence >= sequence) {
            return true;
        }
    }
    
    return false;
}

bool WALReader::read_buffer(size_t bytes, void* dest) {
    if (buffer_offset_ + bytes > read_buffer_.size()) {
        // Need to read more from file
        size_t remaining = read_buffer_.size() - buffer_offset_;
        if (remaining > 0) {
            ::read(fd_, read_buffer_.data(), remaining);
        }
        
        // Read new buffer
        ssize_t bytes_read = ::read(fd_, read_buffer_.data(), read_buffer_.size());
        if (bytes_read <= 0) return false;
        
        buffer_offset_ = 0;
    }
    
    if (buffer_offset_ + bytes <= read_buffer_.size()) {
        std::memcpy(dest, read_buffer_.data() + buffer_offset_, bytes);
        buffer_offset_ += bytes;
        return true;
    }
    
    return false;
}

// WAL Implementation
WAL::WAL(uint32_t shard_id, const std::string& data_dir, ShardMemoryPool* pool)
    : shard_id_(shard_id), data_dir_(data_dir), memory_pool_(pool), running_(false) {
}

WAL::~WAL() {
    stop();
}

bool WAL::start() {
    if (running_) return true;
    
    // Create data directory if it doesn't exist
    std::filesystem::create_directories(data_dir_);
    
    // Open WAL file
    std::string wal_path = data_dir_ + "/wal_" + std::to_string(shard_id_) + ".log";
    writer_ = std::make_unique<WALWriter>(wal_path, memory_pool_);
    
    if (!writer_->is_open()) {
        std::cerr << "Failed to open WAL file: " << wal_path << std::endl;
        return false;
    }
    
    running_ = true;
    return true;
}

void WAL::stop() {
    if (!running_) return;
    
    running_ = false;
    // Writer will be closed automatically in destructor
}

bool WAL::write_put(const Key& key, const Value& value) {
    if (!running_) return false;
    
    WALRecord record;
    record.sequence = writer_->next_sequence();
    record.op_type = OperationType::PUT;
    record.key_size = key.size();
    record.value_size = value.size();
    
    // Calculate checksum manually
    uint32_t checksum = 0;
    checksum ^= record.sequence;
    checksum ^= static_cast<uint32_t>(record.op_type);
    checksum ^= record.key_size;
    checksum ^= record.value_size;
    
    for (char c : key) {
        checksum ^= static_cast<uint32_t>(c);
    }
    
    for (char c : value) {
        checksum ^= static_cast<uint32_t>(c);
    }
    
    record.checksum = checksum;
    
    return writer_->write_record(record, key, value);
}

bool WAL::write_delete(const Key& key) {
    if (!running_) return false;
    
    WALRecord record;
    record.sequence = writer_->next_sequence();
    record.op_type = OperationType::DELETE;
    record.key_size = key.size();
    record.value_size = 0;
    
    // Calculate checksum manually
    uint32_t checksum = 0;
    checksum ^= record.sequence;
    checksum ^= static_cast<uint32_t>(record.op_type);
    checksum ^= record.key_size;
    checksum ^= record.value_size;
    
    for (char c : key) {
        checksum ^= static_cast<uint32_t>(c);
    }
    
    record.checksum = checksum;
    
    return writer_->write_record(record, key, "");
}

bool WAL::flush() {
    if (!running_ || !writer_) return false;
    return writer_->flush();
}

void WAL::checkpoint(uint64_t sequence) {
    // Simple checkpoint implementation - just mark the sequence
    checkpointed_sequence_ = sequence;
}

} // namespace nscfstore
