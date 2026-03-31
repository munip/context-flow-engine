#pragma once

#include "common.h"
#include <atomic>
#include <vector>
#include <memory>
#include <cstddef>

namespace nscfstore {

class ArenaPool {
public:
    explicit ArenaPool(size_t block_size = 1024 * 1024); // 1MB blocks
    
    void* allocate(size_t size);
    void reset();
    size_t size() const { return allocated_count_.load(); }
    size_t capacity() const { return pool_size_; }
    bool empty() const { return allocated_count_.load() == 0; }
    size_t used() const { return used_.load(); }
    
private:
    struct Block {
        std::unique_ptr<char[]> data;
        size_t size;
        size_t used;
        Block* next;
    };
    
    std::atomic<Block*> current_block_;
    std::atomic<size_t> allocated_count_;
    size_t pool_size_;
    std::atomic<size_t> used_;
    std::atomic<size_t> capacity_;
    const size_t block_size_;
    std::mutex block_mutex_;
    
    Block* allocate_block();
};

template<typename T>
class ObjectPool {
public:
    explicit ObjectPool(size_t initial_size = 1000) {
        for (size_t i = 0; i < initial_size; ++i) {
            pool_.push(new T());
        }
    }
    
    ~ObjectPool() {
        while (auto* obj = pool_.pop()) {
            delete obj;
        }
    }
    
    T* acquire() {
        if (auto* obj = pool_.pop()) {
            return obj;
        }
        return new T();
    }
    
    void release(T* obj) {
        if (obj) {
            obj->reset(); // Reset object state if needed
            pool_.push(obj);
        }
    }
    
    // Get pool statistics
    size_t size() const { 
        // Approximate size - this is a simplified implementation
        return 1000; // Return initial size as approximation
    }
    
private:
    struct Node {
        T* obj;
        std::atomic<Node*> next;
    };
    
    std::atomic<Node*> head_;
    
    class LockFreeStack {
    public:
        void push(T* obj) {
            Node* new_node = new Node{obj, nullptr};
            Node* old_head = head_.load();
            do {
                new_node->next = old_head;
            } while (!head_.compare_exchange_weak(old_head, new_node));
        }
        
        T* pop() {
            Node* old_head = head_.load();
            Node* new_head;
            do {
                if (!old_head) return nullptr;
                new_head = old_head->next.load();
            } while (!head_.compare_exchange_weak(old_head, new_head));
            
            T* obj = old_head->obj;
            delete old_head;
            return obj;
        }
        
    private:
        std::atomic<Node*> head_{nullptr};
    };
    
    LockFreeStack pool_;
};

class BlockPool {
public:
    explicit BlockPool(size_t block_size = 4096, size_t initial_blocks = 1000);
    
    void* allocate_block();
    void release_block(void* block);
    
private:
    struct BlockHeader {
        std::atomic<BlockHeader*> next;
    };
    
    std::atomic<BlockHeader*> free_list_;
    const size_t block_size_;
    std::vector<std::unique_ptr<char[]>> allocations_;
};

class ShardMemoryPool {
public:
    explicit ShardMemoryPool(uint32_t shard_id, size_t memory_size);
    
    ArenaPool& memtable_arena() { return memtable_arena_; }
    ArenaPool& wal_buffer_pool() { return wal_buffer_pool_; }
    BlockPool& sstable_blocks() { return sstable_blocks_; }
    ObjectPool<Request>& request_pool() { return request_pool_; }
    ObjectPool<Response>& response_pool() { return response_pool_; }
    
    size_t total_used() const;
    size_t total_capacity() const;
    
private:
    uint32_t shard_id_;
    ArenaPool memtable_arena_;
    ArenaPool wal_buffer_pool_;
    BlockPool sstable_blocks_;
    ObjectPool<Request> request_pool_;
    ObjectPool<Response> response_pool_;
};

} // namespace nscfstore
