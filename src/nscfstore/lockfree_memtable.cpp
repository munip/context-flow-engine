#include "nscfstore/write_path.h"
#include "nscfstore/memtable.h"
#include <random>
#include <algorithm>
#include <chrono>

namespace nscfstore {

// LockFreeMemtable Implementation
template<typename K, typename V>
LockFreeMemtable<K, V>::LockFreeMemtable(ShardMemoryPool* pool, size_t threshold_bytes)
    : memory_pool_(pool),
      threshold_bytes_(threshold_bytes),
      rng_(std::random_device{}()),
      level_dist_(0.25) { // 25% probability for each additional level {
    
    // Create head node with maximum level
    head_ = new SkipListNode<K, V>(K{}, V{}, MAX_SKIP_LIST_LEVELS - 1);
    max_level_.store(1);
    
    // Initialize epochs for epoch-based reclamation
    for (auto& epoch : epochs_) {
        epoch.epoch.store(0);
    }
}

template<typename K, typename V>
LockFreeMemtable<K, V>::~LockFreeMemtable() {
    // Cleanup all nodes
    SkipListNode<K, V>* current = head_;
    while (current) {
        SkipListNode<K, V>* next = current->next[0].load();
        delete current;
        current = next;
    }
    
    // Cleanup garbage
    cleanup_garbage();
}

template<typename K, typename V>
bool LockFreeMemtable<K, V>::put(const K& key, const V& value) {
    if (immutable_.load()) {
        return false; // Cannot write to immutable memtable
    }
    
    enter_epoch();
    
    std::vector<SkipListNode<K, V>*> preds(MAX_SKIP_LIST_LEVELS);
    std::vector<SkipListNode<K, V>*> succs(MAX_SKIP_LIST_LEVELS);
    
    while (true) {
        // Find predecessors and successors
        SkipListNode<K, V>* node = find_predecessors(key, preds, succs);
        
        if (node && !node->marked.load()) {
            // Node already exists, update value
            V old_value = node->value;
            node->value = value;
            
            // Update size statistics
            size_t old_size = key.size() + old_value.size();
            size_t new_size = key.size() + value.size();
            size_bytes_.fetch_add(new_size - old_size);
            
            leave_epoch();
            return true;
        }
        
        // Create new node
        int node_level = random_level();
        SkipListNode<K, V>* new_node = new SkipListNode<K, V>(key, value, node_level);
        
        // Set next pointers
        for (int i = 0; i <= node_level; ++i) {
            new_node->next[i].store(succs[i]);
        }
        
        // Link new node into the list
        bool linked = true;
        for (int i = 0; i <= node_level; ++i) {
            if (!preds[i]->next[i].compare_exchange_weak(succs[i], new_node)) {
                linked = false;
                break;
            }
        }
        
        if (linked) {
            // Successfully linked
            entry_count_.fetch_add(1);
            size_bytes_.fetch_add(key.size() + value.size());
            
            // Update max level if necessary
            int current_max = max_level_.load();
            while (node_level > current_max) {
                if (max_level_.compare_exchange_weak(current_max, node_level + 1)) {
                    break;
                }
            }
            
            leave_epoch();
            return true;
        }
        
        // CAS failed, retry
        add_to_garbage(new_node);
    }
}

template<typename K, typename V>
bool LockFreeMemtable<K, V>::get(const K& key, V& value) {
    enter_epoch();
    
    SkipListNode<K, V>* current = head_;
    
    // Start from the highest level
    int level = max_level_.load() - 1;
    
    while (level >= 0) {
        // Find the first node with key >= target key at this level
        while (current->next[level].load() && 
               current->next[level].load()->key < key) {
            current = current->next[level].load();
        }
        
        level--;
    }
    
    // Move to next node at level 0
    current = current->next[0].load();
    
    if (current && !current->marked.load() && current->key == key) {
        value = current->value;
        leave_epoch();
        return true;
    }
    
    leave_epoch();
    return false;
}

template<typename K, typename V>
bool LockFreeMemtable<K, V>::remove(const K& key) {
    if (immutable_.load()) {
        return false;
    }
    
    enter_epoch();
    
    std::vector<SkipListNode<K, V>*> preds(MAX_SKIP_LIST_LEVELS);
    std::vector<SkipListNode<K, V>*> succs(MAX_SKIP_LIST_LEVELS);
    
    SkipListNode<K, V>* node = find_predecessors(key, preds, succs);
    
    if (!node || node->marked.load()) {
        leave_epoch();
        return false; // Node not found
    }
    
    // Mark node for deletion
    if (!mark_for_removal(node, preds, succs)) {
        leave_epoch();
        return false;
    }
    
    // Physically remove node
    for (int i = 0; i < node->level; ++i) {
        preds[i]->next[i].compare_exchange_strong(succs[i], node->next[i].load());
    }
    
    // Update statistics
    entry_count_.fetch_sub(1);
    size_bytes_.fetch_sub(key.size() + node->value.size());
    
    // Add to garbage for later cleanup
    add_to_garbage(node);
    
    leave_epoch();
    return true;
}

template<typename K, typename V>
int LockFreeMemtable<K, V>::random_level() {
    int level = 0;
    while (level_dist_(rng_) && level < MAX_SKIP_LIST_LEVELS - 1) {
        level++;
    }
    return level;
}

template<typename K, typename V>
SkipListNode<K, V>* LockFreeMemtable<K, V>::find_predecessors(
    const K& key, 
    std::vector<SkipListNode<K, V>*>& preds,
    std::vector<SkipListNode<K, V>*>& succs) {
    
    SkipListNode<K, V>* pred = head_;
    bool found = false;
    SkipListNode<K, V>* found_node = nullptr;
    
    // Start from the highest level
    int level = max_level_.load() - 1;
    
    while (level >= 0) {
        SkipListNode<K, V>* curr = pred->next[level].load();
        
        // Find predecessor at this level
        while (curr && (curr->marked.load() || curr->key < key)) {
            pred = curr;
            curr = pred->next[level].load();
        }
        
        preds[level] = pred;
        
        if (curr && !curr->marked.load() && curr->key == key) {
            found = true;
            found_node = curr;
        }
        
        succs[level] = curr;
        level--;
    }
    
    return found ? found_node : nullptr;
}

template<typename K, typename V>
bool LockFreeMemtable<K, V>::mark_for_removal(
    SkipListNode<K, V>* node,
    std::vector<SkipListNode<K, V>*>& preds,
    std::vector<SkipListNode<K, V>*>& succs) {
    
    // Try to mark the node
    bool expected = false;
    if (!node->marked.compare_exchange_strong(expected, true)) {
        return false; // Already marked by another thread
    }
    
    return true;
}

template<typename K, typename V>
void LockFreeMemtable<K, V>::add_to_garbage(SkipListNode<K, V>* node) {
    SkipListNode<K, V>* head = garbage_list_.load();
    do {
        node->next[0].store(head);
    } while (!garbage_list_.compare_exchange_weak(head, node));
}

template<typename K, typename V>
void LockFreeMemtable<K, V>::cleanup_garbage() {
    SkipListNode<K, V>* current = garbage_list_.exchange(nullptr);
    
    while (current) {
        SkipListNode<K, V>* next = current->next[0].load();
        delete current;
        current = next;
    }
}

template<typename K, typename V>
void LockFreeMemtable<K, V>::enter_epoch() {
    // Simple epoch-based reclamation
    // In a production system, this would be more sophisticated
}

template<typename K, typename V>
void LockFreeMemtable<K, V>::leave_epoch() {
    // Simple epoch-based reclamation
    // In a production system, this would be more sophisticated
}

template<typename K, typename V>
void LockFreeMemtable<K, V>::advance_epoch() {
    int current = current_epoch_.load();
    int next = (current + 1) % epochs_.size();
    
    // Advance to next epoch
    current_epoch_.store(next);
    
    // Cleanup retired nodes from the old epoch
    auto& old_epoch = epochs_[next];
    std::lock_guard<std::mutex> lock(old_epoch.mutex);
    
    for (auto* node : old_epoch.retired_nodes) {
        delete node;
    }
    old_epoch.retired_nodes.clear();
}

template<typename K, typename V>
void LockFreeMemtable<K, V>::reset() {
    // Clear all nodes except head
    SkipListNode<K, V>* current = head_->next[0].load();
    while (current) {
        SkipListNode<K, V>* next = current->next[0].load();
        add_to_garbage(current);
        current = next;
    }
    
    // Reset head pointers
    for (int i = 0; i < MAX_SKIP_LIST_LEVELS; ++i) {
        head_->next[i].store(nullptr);
    }
    
    // Reset statistics
    entry_count_.store(0);
    size_bytes_.store(0);
    max_level_.store(1);
    immutable_.store(false);
    
    // Cleanup garbage
    cleanup_garbage();
}

template<typename K, typename V>
std::vector<std::pair<K, V>> LockFreeMemtable<K, V>::get_all_entries() {
    std::vector<std::pair<K, V>> entries;
    
    enter_epoch();
    
    SkipListNode<K, V>* current = head_->next[0].load();
    while (current) {
        if (!current->marked.load()) {
            entries.emplace_back(current->key, current->value);
        }
        current = current->next[0].load();
    }
    
    leave_epoch();
    
    // Sort by key for SSTable creation
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    
    return entries;
}

template<typename K, typename V>
std::vector<std::pair<K, V>> LockFreeMemtable<K, V>::get_range(
    const K& start_key, const K& end_key) {
    
    std::vector<std::pair<K, V>> entries;
    
    enter_epoch();
    
    SkipListNode<K, V>* current = head_;
    
    // Find start position
    int level = max_level_.load() - 1;
    while (level >= 0) {
        while (current->next[level].load() && 
               current->next[level].load()->key < start_key) {
            current = current->next[level].load();
        }
        level--;
    }
    
    // Scan range
    current = current->next[0].load();
    while (current && !current->marked.load() && current->key <= end_key) {
        entries.emplace_back(current->key, current->value);
        current = current->next[0].load();
    }
    
    leave_epoch();
    
    return entries;
}

// Iterator Implementation
template<typename K, typename V>
LockFreeMemtable<K, V>::Iterator::Iterator(LockFreeMemtable* memtable, const K& start_key)
    : memtable_(memtable), start_key_(start_key) {
    current_ = find_start_node(start_key);
}

template<typename K, typename V>
bool LockFreeMemtable<K, V>::Iterator::next() {
    if (!current_) {
        return false;
    }
    
    current_ = current_->next[0].load();
    
    // Skip marked nodes
    while (current_ && current_->marked.load()) {
        current_ = current_->next[0].load();
    }
    
    return current_ != nullptr;
}

template<typename K, typename V>
SkipListNode<K, V>* LockFreeMemtable<K, V>::Iterator::find_start_node(const K& key) {
    SkipListNode<K, V>* current = memtable_->head_;
    
    int level = memtable_->max_level_.load() - 1;
    while (level >= 0) {
        while (current->next[level].load() && 
               current->next[level].load()->key < key) {
            current = current->next[level].load();
        }
        level--;
    }
    
    current = current->next[0].load();
    
    // Skip marked nodes
    while (current && current->marked.load()) {
        current = current->next[0].load();
    }
    
    return current;
}

// Explicit template instantiations
template class LockFreeMemtable<Key, Value>;

// LockFreeSkipList Implementation
template<typename Key, typename Value>
LockFreeSkipList<Key, Value>::LockFreeSkipList(ShardMemoryPool* pool) 
    : memory_pool_(pool) {
    // Create head node
    head_ = new Node(Key{}, Value{}, MAX_SKIP_LIST_LEVELS - 1);
    Node* head_ptr = head_.load();
    for (int i = 0; i < MAX_SKIP_LIST_LEVELS; ++i) {
        head_ptr->next[i].store(nullptr);
    }
    size_estimate_.store(0);
}

template<typename Key, typename Value>
LockFreeSkipList<Key, Value>::~LockFreeSkipList() {
    // Cleanup all nodes
    Node* current = head_.load();
    while (current) {
        Node* next = current->next[0].load();
        delete current;
        current = next;
    }
}

template<typename Key, typename Value>
bool LockFreeSkipList<Key, Value>::insert(const Key& key, const Value& value) {
    // Simple implementation for now
    Node* new_node = new Node(key, value, 1);
    Node* prev = head_.load();
    
    // Find insertion point
    while (prev->next[0].load() && prev->next[0].load()->key < key) {
        prev = prev->next[0].load();
    }
    
    new_node->next[0].store(prev->next[0].load());
    prev->next[0].store(new_node);
    size_estimate_.fetch_add(1);
    
    return true;
}

template<typename Key, typename Value>
bool LockFreeSkipList<Key, Value>::find(const Key& key, Value& value) {
    Node* current = head_.load()->next[0].load();
    
    while (current) {
        if (current->key == key && !current->marked.load()) {
            value = current->value;
            return true;
        }
        current = current->next[0].load();
    }
    
    return false;
}

template<typename Key, typename Value>
bool LockFreeSkipList<Key, Value>::remove(const Key& key) {
    Node* prev = head_.load();
    Node* current = head_.load()->next[0].load();
    
    while (current) {
        if (current->key == key) {
            current->marked.store(true);
            prev->next[0].store(current->next[0].load());
            size_estimate_.fetch_sub(1);
            delete current;
            return true;
        }
        prev = current;
        current = current->next[0].load();
    }
    
    return false;
}

// LockFreeSkipList Iterator Implementation
template<typename Key, typename Value>
LockFreeSkipList<Key, Value>::Iterator::Iterator(LockFreeSkipList* list, const Key& start_key) 
    : list_(list) {
    current_ = list_->head_.load()->next[0].load();
    
    // Skip to start key
    while (current_ && current_->key < start_key) {
        current_ = current_->next[0].load();
    }
    
    // Skip marked nodes
    while (current_ && current_->marked.load()) {
        current_ = current_->next[0].load();
    }
}

template<typename Key, typename Value>
bool LockFreeSkipList<Key, Value>::Iterator::next() {
    if (current_) {
        current_ = current_->next[0].load();
        
        // Skip marked nodes
        while (current_ && current_->marked.load()) {
            current_ = current_->next[0].load();
        }
        
        return current_ != nullptr;
    }
    return false;
}

// Explicit template instantiations for LockFreeSkipList
template class LockFreeSkipList<Key, Value>;

} // namespace nscfstore
