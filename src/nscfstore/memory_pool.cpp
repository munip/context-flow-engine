#include "nscfstore/memory_pool.h"
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace nscfstore {

// ArenaPool Implementation
ArenaPool::ArenaPool(size_t block_size) 
    : block_size_(block_size) {
    current_block_.store(nullptr);
    used_.store(0);
    capacity_.store(0);
}

void* ArenaPool::allocate(size_t size) {
    if (size > block_size_) {
        throw std::runtime_error("Allocation too large for arena block");
    }
    
    Block* current = current_block_.load();
    
    // Try to allocate from current block
    if (current && current->used + size <= current->size) {
        void* ptr = current->data.get() + current->used;
        current->used += size;
        used_.fetch_add(size);
        return ptr;
    }
    
    // Need new block
    std::lock_guard<std::mutex> lock(block_mutex_);
    
    // Double-check after acquiring lock
    current = current_block_.load();
    if (current && current->used + size <= current->size) {
        void* ptr = current->data.get() + current->used;
        current->used += size;
        used_.fetch_add(size);
        return ptr;
    }
    
    // Allocate new block
    Block* new_block = allocate_block();
    new_block->used = size;
    
    void* ptr = new_block->data.get();
    new_block->next = current;
    current_block_.store(new_block);
    
    used_.fetch_add(size);
    capacity_.fetch_add(block_size_);
    
    return ptr;
}

void ArenaPool::reset() {
    std::lock_guard<std::mutex> lock(block_mutex_);
    
    Block* current = current_block_.load();
    while (current) {
        Block* next = current->next;
        delete current;
        current = next;
    }
    
    current_block_.store(nullptr);
    used_.store(0);
    capacity_.store(0);
}

ArenaPool::Block* ArenaPool::allocate_block() {
    auto block = std::make_unique<Block>();
    block->data = std::make_unique<char[]>(block_size_);
    block->size = block_size_;
    block->used = 0;
    block->next = nullptr;
    
    Block* raw_ptr = block.release();
    return raw_ptr;
}

// BlockPool Implementation
BlockPool::BlockPool(size_t block_size, size_t initial_blocks) 
    : block_size_(block_size) {
    // Allocate initial blocks
    for (size_t i = 0; i < initial_blocks; ++i) {
        auto allocation = std::make_unique<char[]>(block_size_);
        char* raw_ptr = allocation.get();
        allocations_.push_back(std::move(allocation));
        
        // Add to free list
        BlockHeader* header = reinterpret_cast<BlockHeader*>(raw_ptr);
        header->next = free_list_.load();
        free_list_.store(header);
    }
}

void* BlockPool::allocate_block() {
    BlockHeader* header = free_list_.load();
    BlockHeader* next_header;
    
    do {
        if (!header) {
            // No free blocks, allocate new one
            auto allocation = std::make_unique<char[]>(block_size_);
            char* raw_ptr = allocation.get();
            allocations_.push_back(std::move(allocation));
            return raw_ptr;
        }
        next_header = header->next.load();
    } while (!free_list_.compare_exchange_weak(header, next_header));
    
    return header;
}

void BlockPool::release_block(void* block) {
    BlockHeader* header = reinterpret_cast<BlockHeader*>(block);
    BlockHeader* current;
    
    do {
        current = free_list_.load();
        header->next = current;
    } while (!free_list_.compare_exchange_weak(current, header));
}

// ShardMemoryPool Implementation
ShardMemoryPool::ShardMemoryPool(uint32_t shard_id, size_t memory_size) 
    : shard_id_(shard_id),
      memtable_arena_(memory_size / 4),      // 25% for memtables
      wal_buffer_pool_(memory_size / 8),     // 12.5% for WAL buffers
      sstable_blocks_(4096, memory_size / 4096 / 8), // 12.5% for SSTable blocks
      request_pool_(1000),
      response_pool_(1000) {
}

size_t ShardMemoryPool::total_used() const {
    return memtable_arena_.used() + 
           wal_buffer_pool_.used() + 
           request_pool_.size() * sizeof(Request) +
           response_pool_.size() * sizeof(Response);
}

size_t ShardMemoryPool::total_capacity() const {
    return memtable_arena_.capacity() + 
           wal_buffer_pool_.capacity() + 
           request_pool_.size() * sizeof(Request) +
           response_pool_.size() * sizeof(Response);
}

} // namespace nscfstore
