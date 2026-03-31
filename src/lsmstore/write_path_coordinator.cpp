#include "nscfstore/write_path.h"
#include <chrono>
#include <filesystem>

namespace nscfstore {

// WritePathCoordinator Implementation
WritePathCoordinator::WritePathCoordinator(const std::string& wal_path,
                                         ShardMemoryPool* pool,
                                         size_t memtable_threshold)
    : wal_path_(wal_path),
      memory_pool_(pool),
      memtable_threshold_(memtable_threshold) {
    
    // Initialize statistics
    stats_ = {};
}

WritePathCoordinator::~WritePathCoordinator() {
    stop();
}

bool WritePathCoordinator::start() {
    if (running_.load()) {
        return true;
    }
    
    // Initialize WAL
    wal_ = std::make_unique<DirectIOWAL>(wal_path_, 64 * 1024 * 1024, 256 * 1024 * 1024);
    if (!wal_->open()) {
        std::cerr << "Failed to open WAL" << std::endl;
        return false;
    }
    
    // Initialize active memtable
    active_memtable_ = std::make_unique<LockFreeMemtable<Key, Value>>(memory_pool_, memtable_threshold_);
    
    // Start background flush thread
    running_.store(true);
    flush_thread_ = std::thread(&WritePathCoordinator::flush_worker, this);
    
    return true;
}

void WritePathCoordinator::stop() {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    
    // Wake up flush thread
    flush_cv_.notify_all();
    
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    
    // Flush any remaining data
    if (active_memtable_ && !active_memtable_->is_empty()) {
        flush_memtable_to_sstable();
    }
    
    // Close WAL
    if (wal_) {
        wal_->close();
    }
}

bool WritePathCoordinator::put(const Key& key, const Value& value) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Check if we need to trigger flush
    if (active_memtable_->is_full()) {
        trigger_flush();
    }
    
    // Get sequence number from WAL
    uint64_t sequence = wal_->get_last_sequence() + 1;
    
    // Write to WAL first (durability)
    if (!write_to_wal(key, value, sequence)) {
        std::cerr << "Failed to write to WAL" << std::endl;
        return false;
    }
    
    // Write to memtable
    if (!write_to_memtable(key, value)) {
        std::cerr << "Failed to write to memtable" << std::endl;
        return false;
    }
    
    // Update statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();
    
    update_put_stats(key.size() + value.size(), latency);
    
    return true;
}

bool WritePathCoordinator::put_batch(const std::vector<std::pair<Key, Value>>& entries) {
    if (entries.empty()) {
        return true;
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Check if we need to trigger flush
    if (active_memtable_->is_full()) {
        trigger_flush();
    }
    
    // Prepare batch data for WAL
    std::vector<std::pair<const void*, size_t>> wal_records;
    std::vector<uint64_t> sequences;
    size_t total_bytes = 0;
    
    for (const auto& [key, value] : entries) {
        // Serialize key-value pair for WAL
        size_t record_size = key.size() + value.size() + sizeof(uint32_t) * 2;
        char* record_data = static_cast<char*>(memory_pool_->memtable_arena().allocate(record_size));
        
        // Write key size and key
        uint32_t key_size = key.size();
        memcpy(record_data, &key_size, sizeof(key_size));
        memcpy(record_data + sizeof(key_size), key.data(), key.size());
        
        // Write value size and value
        uint32_t value_size = value.size();
        memcpy(record_data + sizeof(key_size) + key.size(), &value_size, sizeof(value_size));
        memcpy(record_data + sizeof(key_size) + key.size() + sizeof(value_size), 
               value.data(), value.size());
        
        wal_records.emplace_back(record_data, record_size);
        sequences.push_back(wal_->get_last_sequence() + sequences.size() + 1);
        total_bytes += record_size;
    }
    
    // Write batch to WAL
    if (!wal_->write_batch(wal_records, sequences)) {
        std::cerr << "Failed to write batch to WAL" << std::endl;
        return false;
    }
    
    // Write batch to memtable
    for (const auto& [key, value] : entries) {
        if (!write_to_memtable(key, value)) {
            std::cerr << "Failed to write batch entry to memtable" << std::endl;
            return false;
        }
    }
    
    // Update statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();
    
    update_put_stats(total_bytes, latency);
    
    return true;
}

void WritePathCoordinator::trigger_flush() {
    if (flush_in_progress_.load()) {
        return; // Flush already in progress
    }
    
    std::lock_guard<std::mutex> lock(flush_mutex_);
    flush_cv_.notify_one();
}

WritePathCoordinator::WriteStats WritePathCoordinator::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    WriteStats stats = stats_;
    
    // Add WAL statistics
    if (wal_) {
        auto wal_stats = wal_->get_stats();
        stats.wal_writes = wal_stats.total_writes;
        stats.wal_syncs = wal_stats.total_syncs;
        stats.wal_write_latency_us = wal_stats.avg_write_latency_us;
        stats.wal_sync_latency_us = wal_stats.avg_flush_latency_us; // sync latency similar to flush
    }
    
    return stats;
}

void WritePathCoordinator::reset_stats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = {};
    
    if (wal_) {
        wal_->reset_stats();
    }
}

bool WritePathCoordinator::write_to_wal(const Key& key, const Value& value, uint64_t sequence) {
    // Serialize key-value pair for WAL
    size_t record_size = key.size() + value.size() + sizeof(uint32_t) * 2;
    char* record_data = static_cast<char*>(memory_pool_->memtable_arena().allocate(record_size));
    
    // Write key size and key
    uint32_t key_size = key.size();
    memcpy(record_data, &key_size, sizeof(key_size));
    memcpy(record_data + sizeof(key_size), key.data(), key.size());
    
    // Write value size and value
    uint32_t value_size = value.size();
    memcpy(record_data + sizeof(key_size) + key.size(), &value_size, sizeof(value_size));
    memcpy(record_data + sizeof(key_size) + key.size() + sizeof(value_size), 
           value.data(), value.size());
    
    // Write to WAL
    bool success = wal_->write_record(record_data, record_size, sequence);
    
    // Note: record_data will be freed when the arena is reset
    return success;
}

bool WritePathCoordinator::write_to_memtable(const Key& key, const Value& value) {
    return active_memtable_->put(key, value);
}

void WritePathCoordinator::flush_worker() {
    while (running_.load()) {
        std::unique_lock<std::mutex> lock(flush_mutex_);
        
        flush_cv_.wait(lock, [this] { 
            return active_memtable_->is_full() || !running_.load(); 
        });
        
        if (!running_.load()) {
            break;
        }
        
        if (active_memtable_->is_full()) {
            lock.unlock();
            
            if (flush_memtable_to_sstable()) {
                update_flush_stats();
            }
        }
    }
}

bool WritePathCoordinator::flush_memtable_to_sstable() {
    if (flush_in_progress_.exchange(true)) {
        return false; // Another flush is in progress
    }
    
    bool success = false;
    
    try {
        // Switch memtables (make current active memtable immutable)
        switch_memtables();
        
        if (immutable_memtable_) {
            // Create SSTable from immutable memtable
            std::string sstable_path = "./data/sstable/level0_" + 
                                      std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::system_clock::now().time_since_epoch()).count()) + 
                                      ".sst";
            
            SSTableLevel0Builder builder(sstable_path, memory_pool_);
            
            if (builder.build_from_memtable(immutable_memtable_.get())) {
                std::cout << "Successfully flushed memtable to SSTable: " << sstable_path << std::endl;
                success = true;
            } else {
                std::cerr << "Failed to flush memtable to SSTable" << std::endl;
            }
            
            // Reset immutable memtable
            immutable_memtable_->reset();
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Exception during flush: " << e.what() << std::endl;
        success = false;
    }
    
    flush_in_progress_.store(false);
    return success;
}

void WritePathCoordinator::switch_memtables() {
    // Mark current active memtable as immutable
    active_memtable_->mark_immutable();
    
    // Move active memtable to immutable
    immutable_memtable_ = std::move(active_memtable_);
    
    // Create new active memtable
    active_memtable_ = std::make_unique<LockFreeMemtable<Key, Value>>(memory_pool_, memtable_threshold_);
}

void WritePathCoordinator::update_put_stats(size_t bytes, uint64_t latency_us) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    stats_.total_puts++;
    stats_.total_bytes_written += bytes;
    
    // Update average latency
    uint64_t total_latency = stats_.avg_put_latency_us * (stats_.total_puts - 1) + latency_us;
    stats_.avg_put_latency_us = static_cast<double>(total_latency) / stats_.total_puts;
}

void WritePathCoordinator::update_flush_stats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.memtable_flushes++;
}

// SSTableLevel0Builder Implementation
SSTableLevel0Builder::SSTableLevel0Builder(const std::string& sstable_path,
                                         ShardMemoryPool* pool)
    : sstable_path_(sstable_path),
      memory_pool_(pool),
      fd_(-1),
      entry_count_(0) {
}

SSTableLevel0Builder::~SSTableLevel0Builder() {
    if (fd_ != -1) {
        ::close(fd_);
    }
}

bool SSTableLevel0Builder::build_from_memtable(LockFreeMemtable<Key, Value>* memtable) {
    if (!memtable) {
        return false;
    }
    
    // Get all entries from memtable
    auto entries = memtable->get_all_entries();
    return build_from_entries(entries);
}

bool SSTableLevel0Builder::build_from_entries(const std::vector<std::pair<Key, Value>>& entries) {
    if (entries.empty()) {
        return false;
    }
    
    // Ensure directory exists
    if (!ensure_directory_exists(sstable_path_)) {
        return false;
    }
    
    // Open SSTable file
    fd_ = ::open(sstable_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ == -1) {
        std::cerr << "Failed to create SSTable file: " << strerror(errno) << std::endl;
        return false;
    }
    
    entry_count_ = entries.size();
    
    // Write header placeholder (will be updated later)
    off_t header_pos = ::lseek(fd_, 0, SEEK_SET);
    SSTableHeader header{};
    if (::write(fd_, &header, sizeof(header)) != sizeof(header)) {
        std::cerr << "Failed to write SSTable header" << std::endl;
        return false;
    }
    
    // Write data block
    if (!write_data_block(entries)) {
        return false;
    }
    
    // Write index block
    std::vector<IndexEntry> index;
    for (const auto& [key, value] : entries) {
        IndexEntry entry;
        entry.key = key;
        // Note: In a real implementation, we would track offsets during data write
        entry.offset = 0; // Placeholder
        entry.size = value.size();
        entry.checksum = calculate_checksum(value.data(), value.size());
        index.push_back(entry);
    }
    
    if (!write_index_block(index)) {
        return false;
    }
    
    // Write footer
    if (!write_footer()) {
        return false;
    }
    
    // Update header with correct offsets
    header.magic = 0x53535441; // "SSTA"
    header.version = 1;
    header.entry_count = entry_count_;
    header.data_offset = sizeof(SSTableHeader);
    header.index_offset = ::lseek(fd_, 0, SEEK_CUR) - index.size() * sizeof(IndexEntry);
    header.footer_offset = ::lseek(fd_, 0, SEEK_CUR);
    header.checksum = 0; // Calculate if needed
    
    // Rewrite header
    ::lseek(fd_, header_pos, SEEK_SET);
    if (::write(fd_, &header, sizeof(header)) != sizeof(header)) {
        std::cerr << "Failed to update SSTable header" << std::endl;
        return false;
    }
    
    ::fsync(fd_);
    ::close(fd_);
    fd_ = -1;
    
    return true;
}

size_t SSTableLevel0Builder::get_file_size() const {
    struct stat st;
    if (::stat(sstable_path_.c_str(), &st) == 0) {
        return st.st_size;
    }
    return 0;
}

bool SSTableLevel0Builder::write_data_block(const std::vector<std::pair<Key, Value>>& entries) {
    for (const auto& [key, value] : entries) {
        // Write key size and key
        uint32_t key_size = key.size();
        if (::write(fd_, &key_size, sizeof(key_size)) != sizeof(key_size)) {
            return false;
        }
        if (::write(fd_, key.data(), key.size()) != static_cast<ssize_t>(key.size())) {
            return false;
        }
        
        // Write value size and value
        uint32_t value_size = value.size();
        if (::write(fd_, &value_size, sizeof(value_size)) != sizeof(value_size)) {
            return false;
        }
        if (::write(fd_, value.data(), value.size()) != static_cast<ssize_t>(value.size())) {
            return false;
        }
    }
    
    return true;
}

bool SSTableLevel0Builder::write_index_block(const std::vector<IndexEntry>& index) {
    for (const auto& entry : index) {
        // Write key
        uint32_t key_size = entry.key.size();
        if (::write(fd_, &key_size, sizeof(key_size)) != sizeof(key_size)) {
            return false;
        }
        if (::write(fd_, entry.key.data(), key.size()) != static_cast<ssize_t>(key.size())) {
            return false;
        }
        
        // Write offset, size, and checksum
        if (::write(fd_, &entry.offset, sizeof(entry.offset)) != sizeof(entry.offset)) {
            return false;
        }
        if (::write(fd_, &entry.size, sizeof(entry.size)) != sizeof(entry.size)) {
            return false;
        }
        if (::write(fd_, &entry.checksum, sizeof(entry.checksum)) != sizeof(entry.checksum)) {
            return false;
        }
    }
    
    return true;
}

bool SSTableLevel0Builder::write_footer() {
    // Write magic number and version
    uint32_t magic = 0x53535441; // "SSTA"
    uint32_t version = 1;
    
    if (::write(fd_, &magic, sizeof(magic)) != sizeof(magic)) {
        return false;
    }
    if (::write(fd_, &version, sizeof(version)) != sizeof(version)) {
        return false;
    }
    
    return true;
}

uint32_t SSTableLevel0Builder::calculate_checksum(const void* data, size_t size) {
    // Simple CRC32 implementation
    uint32_t crc = 0xFFFFFFFF;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    
    for (size_t i = 0; i < size; ++i) {
        crc ^= bytes[i];
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    
    return ~crc;
}

bool SSTableLevel0Builder::ensure_directory_exists(const std::string& path) {
    std::filesystem::path file_path(path);
    std::filesystem::path dir_path = file_path.parent_path();
    
    return std::filesystem::create_directories(dir_path);
}

} // namespace nscfstore
