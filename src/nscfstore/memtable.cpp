#include "nscfstore/memtable.h"
#include <iostream>

namespace nscfstore {

// Memtable Implementation
Memtable::Memtable(ShardMemoryPool* pool) 
    : memory_pool_(pool), skiplist_(std::make_unique<LockFreeSkipList<Key, Value>>(pool)) {
}

Memtable::~Memtable() {
    // Clear all entries
    clear();
}

bool Memtable::put(const Key& key, const Value& value) {
    if (!skiplist_) return false;
    
    bool result = skiplist_->insert(key, value);
    if (result) {
        size_bytes_ += key.size() + value.size();
        entry_count_++;
    }
    
    return result;
}

bool Memtable::get(const Key& key, Value& value) {
    if (!skiplist_) return false;
    
    return skiplist_->find(key, value);
}

bool Memtable::remove(const Key& key) {
    if (!skiplist_) return false;
    
    bool result = skiplist_->remove(key);
    if (result) {
        entry_count_--;
        // Note: size_bytes_ is not decremented since we don't know the original value size
        // In a real implementation, we'd need to track this properly
    }
    
    return result;
}

void Memtable::clear() {
    if (skiplist_) {
        skiplist_.reset();
        skiplist_ = std::make_unique<LockFreeSkipList<Key, Value>>(memory_pool_);
    }
    
    size_bytes_ = 0;
    entry_count_ = 0;
}

bool Memtable::is_full() const {
    return size_bytes_.load() >= MAX_MEMTABLE_SIZE;
}

std::vector<std::pair<Key, Value>> Memtable::get_all_entries() {
    std::vector<std::pair<Key, Value>> results;
    
    if (!skiplist_) return results;
    
    auto iter = skiplist_->begin();
    while (iter.is_valid()) {
        results.emplace_back(iter.key(), iter.value());
        iter.next();
    }
    
    return results;
}

Memtable::Scanner Memtable::scan(const Key& start_key, const Key& end_key) {
    return Scanner(this, start_key, end_key);
}

// Memtable::Scanner Implementation
Memtable::Scanner::Scanner(Memtable* memtable, const Key& start_key, const Key& end_key)
    : end_key_(end_key), iter_(memtable->skiplist_->begin(start_key)) {
}

bool Memtable::Scanner::next() {
    return iter_.next();
}

const Key& Memtable::Scanner::key() const {
    return iter_.key();
}

const Value& Memtable::Scanner::value() const {
    return iter_.value();
}

bool Memtable::Scanner::is_valid() const {
    return iter_.is_valid() && iter_.key() <= end_key_;
}

} // namespace nscfstore
