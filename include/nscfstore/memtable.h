#pragma once

#include "common.h"
#include "memory_pool.h"
#include <atomic>
#include <vector>
#include <random>

namespace nscfstore {

template<typename Key, typename Value>
class LockFreeSkipList {
public:
    struct Node {
        Key key;
        Value value;
        std::atomic<Node*> next[MAX_SKIP_LIST_LEVELS];
        std::atomic<bool> marked{false};
        uint8_t level;
        
        Node(const Key& k, const Value& v, uint8_t lvl) 
            : key(k), value(v), level(lvl) {
            for (int i = 0; i <= lvl; ++i) {
                next[i].store(nullptr);
            }
        }
    };
    
    explicit LockFreeSkipList(ShardMemoryPool* pool);
    ~LockFreeSkipList();
    
    bool insert(const Key& key, const Value& value);
    bool find(const Key& key, Value& value);
    bool remove(const Key& key);
    
    // Iterator for range scans
    class Iterator {
    public:
        Iterator(LockFreeSkipList* list, const Key& start_key = "");
        bool next();
        const Key& key() const { return current_->key; }
        const Value& value() const { return current_->value; }
        bool is_valid() const { return current_ != nullptr; }
        
    private:
        Node* current_;
        LockFreeSkipList* list_;
        Key start_key_;
    };
    
    Iterator begin(const Key& start_key = "") { return Iterator(this, start_key); }
    
    size_t size_estimate() const { return size_estimate_.load(); }
    void reset_size_estimate() { size_estimate_.store(0); }
    
private:
    std::atomic<Node*> head_;
    std::atomic<int> max_level_;
    std::atomic<size_t> size_estimate_;
    ShardMemoryPool* memory_pool_;
    
    std::mt19937 rng_;
    std::uniform_int_distribution<int> level_dist_;
    
    int random_level();
    Node* find_predecessors(const Key& key, std::vector<Node*>& preds, std::vector<Node*>& succs);
    bool mark_for_removal(Node* node, std::vector<Node*>& preds, std::vector<Node*>& succs);
};

class Memtable {
public:
    explicit Memtable(ShardMemoryPool* pool);
    ~Memtable();
    
    bool put(const Key& key, const Value& value);
    bool get(const Key& key, Value& value);
    bool remove(const Key& key);
    
    // Range scan
    class Scanner {
    public:
        Scanner(Memtable* memtable, const Key& start_key, const Key& end_key);
        bool next();
        const Key& key() const;
        const Value& value() const;
        bool is_valid() const;
        
    private:
        LockFreeSkipList<Key, Value>::Iterator iter_;
        Key end_key_;
    };
    
    Scanner scan(const Key& start_key, const Key& end_key);
    
    // Memtable management
    size_t size_bytes() const { return size_bytes_.load(); }
    size_t entry_count() const { return entry_count_.load(); }
    bool is_full() const;
    void clear();
    
    // For compaction
    std::vector<std::pair<Key, Value>> get_all_entries();
    
private:
    std::unique_ptr<LockFreeSkipList<Key, Value>> skiplist_;
    std::atomic<size_t> size_bytes_{0};
    std::atomic<size_t> entry_count_{0};
    ShardMemoryPool* memory_pool_;
    
    static constexpr size_t MAX_MEMTABLE_SIZE = 128 * 1024 * 1024; // 128MB
};

} // namespace nscfstore
