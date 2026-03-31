#include "nscfstore/read_path.h"
#include <algorithm>
#include <chrono>

namespace nscfstore {

// GlobalRowCache Implementation
GlobalRowCache::GlobalRowCache(size_t capacity_bytes)
    : capacity_bytes_(capacity_bytes) {
    
    // Start eviction thread
    eviction_thread_ = std::thread(&GlobalRowCache::eviction_worker, this);
}

GlobalRowCache::~GlobalRowCache() {
    running_.store(false);
    eviction_cv_.notify_all();
    
    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
}

bool GlobalRowCache::get(const Key& key, Value& value) {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    
    auto it = cache_.find(key);
    if (it != cache_.end() && it->second->valid.load()) {
        // Update access statistics
        update_access_stats(it->second.get());
        
        value = it->second->value;
        hits_.fetch_add(1);
        return true;
    }
    
    misses_.fetch_add(1);
    return false;
}

void GlobalRowCache::put(const Key& key, const Value& value) {
    size_t entry_size = key.size() + value.size() + sizeof(CacheEntry);
    
    // Check if entry is too large for cache
    if (entry_size > capacity_bytes_) {
        return; // Don't cache very large entries
    }
    
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    
    // Check if key already exists
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        // Update existing entry
        CacheEntry* entry = it->second.get();
        
        // Update size tracking
        current_usage_.fetch_sub(entry->size_bytes.load());
        current_usage_.fetch_add(entry_size);
        
        entry->value = value;
        entry->size_bytes.store(entry_size);
        entry->access_time.store(get_current_time());
        entry->access_count.store(1);
        entry->valid.store(true);
        
        return;
    }
    
    // Create new entry
    auto entry = std::make_unique<CacheEntry>();
    entry->value = value;
    entry->size_bytes.store(entry_size);
    entry->access_time.store(get_current_time());
    entry->access_count.store(1);
    entry->valid.store(true);
    
    // Check if we need to evict
    while (should_evict() && !cache_.empty()) {
        lock.unlock();
        evict_lru();
        lock.lock();
    }
    
    // Add new entry
    current_usage_.fetch_add(entry_size);
    cache_[key] = std::move(entry);
}

void GlobalRowCache::remove(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        size_t entry_size = it->second->size_bytes.load();
        current_usage_.fetch_sub(entry_size);
        cache_.erase(it);
    }
}

GlobalRowCache::Stats GlobalRowCache::get_stats() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    
    uint64_t total_hits = hits_.load();
    uint64_t total_misses = misses_.load();
    uint64_t total_requests = total_hits + total_misses;
    
    Stats stats;
    stats.hits = total_hits;
    stats.misses = total_misses;
    stats.evictions = evictions_.load();
    stats.hit_rate = total_requests > 0 ? 
                    static_cast<double>(total_hits) / total_requests : 0.0;
    stats.current_usage = current_usage_.load();
    stats.capacity = capacity_bytes_;
    stats.num_entries = cache_.size();
    
    return stats;
}

void GlobalRowCache::reset_stats() {
    hits_.store(0);
    misses_.store(0);
    evictions_.store(0);
}

void GlobalRowCache::clear() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    
    cache_.clear();
    current_usage_.store(0);
}

void GlobalRowCache::resize(size_t new_capacity) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    
    capacity_bytes_ = new_capacity;
    
    // Evict if necessary
    while (should_evict() && !cache_.empty()) {
        lock.unlock();
        evict_lru();
        lock.lock();
    }
}

bool GlobalRowCache::should_evict() const {
    return current_usage_.load() > capacity_bytes_;
}

void GlobalRowCache::evict_lru() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    
    if (cache_.empty()) {
        return;
    }
    
    // Find LRU entry
    auto lru_it = std::min_element(cache_.begin(), cache_.end(),
        [](const auto& a, const auto& b) {
            uint64_t time_a = a.second->access_time.load();
            uint64_t time_b = b.second->access_time.load();
            return time_a < time_b;
        });
    
    if (lru_it != cache_.end()) {
        size_t entry_size = lru_it->second->size_bytes.load();
        current_usage_.fetch_sub(entry_size);
        cache_.erase(lru_it);
        evictions_.fetch_add(1);
    }
}

void GlobalRowCache::eviction_worker() {
    while (running_.load()) {
        std::unique_lock<std::mutex> lock(eviction_mutex_);
        
        // Wait for eviction condition or timeout
        eviction_cv_.wait_for(lock, std::chrono::seconds(1), [this] {
            return should_evict() || !running_.load();
        });
        
        if (!running_.load()) {
            break;
        }
        
        if (should_evict()) {
            lock.unlock();
            evict_lru();
        }
    }
}

void GlobalRowCache::update_access_stats(CacheEntry* entry) {
    uint64_t current_time = get_current_time();
    entry->access_time.store(current_time);
    entry->access_count.fetch_add(1);
}

uint64_t GlobalRowCache::get_current_time() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

} // namespace nscfstore
