#include "nscfstore/write_path.h"
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>

namespace nscfstore {

// DirectIOWAL Implementation
DirectIOWAL::DirectIOWAL(const std::string& file_path, 
                         size_t buffer_size, 
                         size_t segment_size)
    : file_path_(file_path),
      fd_(-1),
      buffer_size_(align_up(buffer_size, ALIGNMENT)),
      segment_size_(align_up(segment_size, ALIGNMENT)),
      write_buffer_(nullptr),
      buffer_offset_(0),
      current_file_offset_(0),
      current_segment_(0) {
    
    // Initialize statistics
    stats_ = {};
    
    // Initialize io_uring
    init_io_uring();
}

DirectIOWAL::~DirectIOWAL() {
    close();
}

bool DirectIOWAL::open() {
    // Ensure directory exists
    std::filesystem::path path(file_path_);
    std::filesystem::create_directories(path.parent_path());
    
    // Open file with O_DIRECT for direct I/O
    fd_ = ::open(file_path_.c_str(), 
                  O_WRONLY | O_CREAT | O_DIRECT | O_APPEND, 
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    
    if (fd_ == -1) {
        std::cerr << "Failed to open WAL file " << file_path_ 
                  << " with O_DIRECT: " << strerror(errno) << std::endl;
        return false;
    }
    
    // Allocate aligned buffer
    write_buffer_ = allocate_aligned_buffer(buffer_size_);
    if (!write_buffer_) {
        std::cerr << "Failed to allocate aligned buffer for WAL" << std::endl;
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    
    // Get current file size
    struct stat st;
    if (::fstat(fd_, &st) == 0) {
        current_file_offset_ = st.st_size;
        current_segment_ = current_file_offset_ / segment_size_;
    }
    
    // Start background flush thread
    running_.store(true);
    flush_thread_ = std::thread(&DirectIOWAL::flush_worker, this);
    
    return true;
}

void DirectIOWAL::close() {
    if (running_.load()) {
        running_.store(false);
        flush_cv_.notify_all();
        
        if (flush_thread_.joinable()) {
            flush_thread_.join();
        }
    }
    
    // Flush any remaining data
    if (buffer_offset_ > 0) {
        flush_buffer();
    }
    
    // Close file
    if (fd_ != -1) {
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
    
    // Free buffer
    if (write_buffer_) {
        free_aligned_buffer(write_buffer_);
        write_buffer_ = nullptr;
    }
    
    // Cleanup io_uring
    if (io_uring_initialized_.load()) {
        io_uring_queue_exit(&ring_);
        io_uring_initialized_.store(false);
    }
}

bool DirectIOWAL::write_record(const void* data, size_t size, uint64_t sequence) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Validate input
    if (!data || size == 0 || size > buffer_size_) {
        std::cerr << "Invalid write record parameters" << std::endl;
        return false;
    }
    
    // Check if we need to rotate segment
    if (current_file_offset_ + size > segment_size_) {
        if (!rotate_segment()) {
            return false;
        }
    }
    
    // Write to buffer
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        
        if (!write_to_buffer(data, size)) {
            return false;
        }
    }
    
    // Update sequence number
    last_sequence_.store(sequence);
    current_file_offset_ += size;
    
    // Update statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();
    
    update_write_stats(size, latency);
    
    return true;
}

bool DirectIOWAL::write_batch(const std::vector<std::pair<const void*, size_t>>& records,
                            const std::vector<uint64_t>& sequences) {
    if (records.size() != sequences.size()) {
        std::cerr << "Records and sequences size mismatch" << std::endl;
        return false;
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    size_t total_bytes = 0;
    
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    
    for (size_t i = 0; i < records.size(); ++i) {
        const auto& [data, size] = records[i];
        
        if (!data || size == 0) {
            continue;
        }
        
        // Check segment rotation
        if (current_file_offset_ + size > segment_size_) {
            if (!rotate_segment()) {
                return false;
            }
        }
        
        if (!write_to_buffer(data, size)) {
            return false;
        }
        
        total_bytes += size;
        current_file_offset_ += size;
        last_sequence_.store(sequences[i]);
    }
    
    // Update statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();
    
    update_write_stats(total_bytes, latency);
    
    return true;
}

bool DirectIOWAL::write_record_async(const void* data, size_t size, uint64_t sequence) {
    if (!io_uring_initialized_.load()) {
        return false;
    }
    
    // For now, fall back to synchronous write
    // In a full implementation, would use io_uring for async writes
    return write_record(data, size, sequence);
}

bool DirectIOWAL::flush_async() {
    if (!io_uring_initialized_.load()) {
        return false;
    }
    
    flush_pending_.store(true);
    flush_cv_.notify_one();
    return true;
}

bool DirectIOWAL::sync_async() {
    if (!io_uring_initialized_.load()) {
        return false;
    }
    
    // For now, fall back to synchronous sync
    return sync();
}

bool DirectIOWAL::flush() {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    
    if (buffer_offset_ == 0) {
        return true; // Nothing to flush
    }
    
    return flush_buffer();
}

bool DirectIOWAL::sync() {
    if (fd_ == -1) {
        return false;
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Flush buffer first
    if (!flush()) {
        return false;
    }
    
    // Sync to disk
    int result = ::fsync(fd_);
    if (result == -1) {
        std::cerr << "Failed to sync WAL: " << strerror(errno) << std::endl;
        return false;
    }
    
    // Update statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();
    
    update_flush_stats(latency);
    
    return true;
}

size_t DirectIOWAL::get_file_size() const {
    if (fd_ == -1) {
        return 0;
    }
    
    struct stat st;
    if (::fstat(fd_, &st) == 0) {
        return st.st_size;
    }
    
    return 0;
}

DirectIOWAL::Stats DirectIOWAL::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void DirectIOWAL::reset_stats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = {};
}

bool DirectIOWAL::init_io_uring() {
    int result = io_uring_queue_init(128, &ring_, 0);
    if (result == 0) {
        io_uring_initialized_.store(true);
        return true;
    }
    
    std::cerr << "Failed to initialize io_uring: " << -result << std::endl;
    return false;
}

bool DirectIOWAL::setup_direct_io() {
    // Direct I/O is already set up in open() with O_DIRECT flag
    return fd_ != -1;
}

void* DirectIOWAL::allocate_aligned_buffer(size_t size) {
    void* buffer = nullptr;
    int result = posix_memalign(&buffer, ALIGNMENT, size);
    
    if (result != 0) {
        return nullptr;
    }
    
    // Advise the kernel to not cache this memory
    madvise(buffer, size, MADV_DONTNEED);
    
    return buffer;
}

void DirectIOWAL::free_aligned_buffer(void* buffer) {
    if (buffer) {
        free(buffer);
    }
}

bool DirectIOWAL::write_to_buffer(const void* data, size_t size) {
    // Check if buffer has enough space
    if (buffer_offset_ + size > buffer_size_) {
        if (!flush_buffer()) {
            return false;
        }
    }
    
    // Copy data to buffer
    memcpy(static_cast<char*>(write_buffer_) + buffer_offset_, data, size);
    buffer_offset_ += size;
    
    return true;
}

bool DirectIOWAL::flush_buffer() {
    if (buffer_offset_ == 0) {
        return true;
    }
    
    // Write buffer to file using direct I/O
    ssize_t written = ::write(fd_, write_buffer_, buffer_offset_);
    
    if (written == -1) {
        std::cerr << "Failed to write WAL buffer: " << strerror(errno) << std::endl;
        return false;
    }
    
    if (static_cast<size_t>(written) != buffer_offset_) {
        std::cerr << "Partial write to WAL: expected " << buffer_offset_ 
                  << ", wrote " << written << std::endl;
        return false;
    }
    
    // Update flushed sequence
    flushed_sequence_.store(last_sequence_.load());
    
    // Reset buffer
    buffer_offset_ = 0;
    
    return true;
}

bool DirectIOWAL::rotate_segment() {
    // Close current file
    if (fd_ != -1) {
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
    
    // Generate new segment file name
    current_segment_++;
    std::string segment_path = file_path_ + "." + std::to_string(current_segment_);
    
    // Open new segment file
    fd_ = ::open(segment_path.c_str(), 
                  O_WRONLY | O_CREAT | O_DIRECT | O_APPEND, 
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    
    if (fd_ == -1) {
        std::cerr << "Failed to open new WAL segment " << segment_path 
                  << ": " << strerror(errno) << std::endl;
        return false;
    }
    
    // Reset file offset
    current_file_offset_ = 0;
    
    return true;
}

void DirectIOWAL::flush_worker() {
    while (running_.load()) {
        std::unique_lock<std::mutex> lock(flush_mutex_);
        
        flush_cv_.wait(lock, [this] { 
            return flush_pending_.load() || !running_.load(); 
        });
        
        if (!running_.load()) {
            break;
        }
        
        flush_pending_.store(false);
        lock.unlock();
        
        // Perform flush
        flush();
    }
}

void DirectIOWAL::update_write_stats(size_t bytes, uint64_t latency_us) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    stats_.total_writes++;
    stats_.total_bytes_written += bytes;
    
    // Update average latency
    uint64_t total_latency = stats_.avg_write_latency_us * (stats_.total_writes - 1) + latency_us;
    stats_.avg_write_latency_us = static_cast<double>(total_latency) / stats_.total_writes;
}

void DirectIOWAL::update_flush_stats(uint64_t latency_us) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    stats_.total_flushes++;
    
    // Update average flush latency
    uint64_t total_latency = stats_.avg_flush_latency_us * (stats_.total_flushes - 1) + latency_us;
    stats_.avg_flush_latency_us = static_cast<double>(total_latency) / stats_.total_flushes;
}

size_t DirectIOWAL::align_up(size_t size, size_t alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
}

void* DirectIOWAL::align_pointer(void* ptr, size_t alignment) {
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    return reinterpret_cast<void*>((addr + alignment - 1) & ~(alignment - 1));
}

} // namespace nscfstore
