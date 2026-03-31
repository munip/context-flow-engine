#include "nscfstore/relationship.h"
#include "nscfstore/shard.h"
#include <algorithm>
#include <queue>
#include <unordered_set>

namespace nscfstore {

// RelationshipManager Implementation
RelationshipManager::RelationshipManager(ShardManager* shard_manager, 
                                       size_t memory_limit_bytes)
    : shard_manager_(shard_manager),
      memory_limit_bytes_(memory_limit_bytes),
      temporal_window_seconds_(3600) {  // Default 1 hour window
    
    // Initialize inference engine
    auto* first_shard = shard_manager_->get_shard_by_id(0);
    if (first_shard) {
        inference_engine_ = std::make_unique<RelationshipInferenceEngine>(
            &first_shard->memory_pool());
    }
    
    // Initialize partitioned bloom filters optimized for 16GB RAM
    size_t bloom_filter_memory = memory_limit_bytes_ / 10; // 10% of total memory
    entity_bloom_filter_ = std::make_unique<PartitionedBloomFilter>(
        1000000, // 1M expected entities
        0.01,    // 1% false positive rate
        16       // 16 partitions for memory efficiency
    );
    
    relationship_bloom_filter_ = std::make_unique<PartitionedBloomFilter>(
        10000000, // 10M expected relationships
        0.01,     // 1% false positive rate
        16        // 16 partitions
    );
    
    // Resize bloom filters to fit memory limit
    entity_bloom_filter_->resize_for_memory_limit(bloom_filter_memory / 2);
    relationship_bloom_filter_->resize_for_memory_limit(bloom_filter_memory / 2);
}

RelationshipManager::~RelationshipManager() {
    // Cleanup is handled by smart pointers
}

void RelationshipManager::process_event(const Event& event) {
    relationships_processed_.fetch_add(1);
    
    // Check if entity is already known
    bool entity_exists = entity_bloom_filter_->might_contain(event.entity_id);
    if (!entity_exists) {
        entity_bloom_filter_->add(event.entity_id);
    }
    
    // Process explicit relationships from event payload
    for (const auto& explicit_rel : event.explicit_relationships) {
        store_relationship(explicit_rel);
    }
    
    // Infer relationships if none provided or to supplement existing ones
    if (event.explicit_relationships.empty() || 
        event.explicit_relationships.size() < 5) { // Max 5 explicit relationships
        
        auto recent_events = get_recent_events(event.entity_id, temporal_window_seconds_);
        auto inferred_relationships = inference_engine_->infer_relationships(event, recent_events);
        
        for (const auto& inferred_rel : inferred_relationships) {
            store_relationship(inferred_rel);
            relationships_inferred_.fetch_add(1);
        }
    }
    
    // Check memory usage and optimize if needed
    check_memory_usage();
}

std::vector<Relationship> RelationshipManager::get_relationships(const std::string& entity_id) {
    auto wide_row = load_wide_row(entity_id);
    return wide_row.get_all_relationships();
}

std::vector<Relationship> RelationshipManager::get_active_relationships(
    const std::string& entity_id) {
    
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    auto wide_row = load_wide_row(entity_id);
    return wide_row.get_active_relationships(current_time);
}

std::vector<Relationship> RelationshipManager::get_relationships_by_type(
    const std::string& entity_id, RelationshipType type) {
    
    auto wide_row = load_wide_row(entity_id);
    return wide_row.get_relationships_by_type(type);
}

std::vector<std::string> RelationshipManager::get_connected_entities(
    const std::string& entity_id, int max_depth) {
    
    std::vector<std::string> connected_entities;
    std::unordered_set<std::string> visited;
    std::queue<std::pair<std::string, int>> queue;
    
    queue.push({entity_id, 0});
    visited.insert(entity_id);
    
    while (!queue.empty()) {
        auto [current_entity, depth] = queue.front();
        queue.pop();
        
        if (depth >= max_depth) {
            continue;
        }
        
        auto relationships = get_active_relationships(current_entity);
        for (const auto& rel : relationships) {
            std::string related_entity = rel.related_id;
            
            if (visited.find(related_entity) == visited.end()) {
                visited.insert(related_entity);
                connected_entities.push_back(related_entity);
                queue.push({related_entity, depth + 1});
            }
        }
    }
    
    return connected_entities;
}

std::vector<std::string> RelationshipManager::get_entities_by_relationship(
    const std::string& entity_id, RelationshipType type, int max_depth) {
    
    std::vector<std::string> result_entities;
    std::unordered_set<std::string> visited;
    std::queue<std::pair<std::string, int>> queue;
    
    queue.push({entity_id, 0});
    visited.insert(entity_id);
    
    while (!queue.empty()) {
        auto [current_entity, depth] = queue.front();
        queue.pop();
        
        if (depth >= max_depth) {
            continue;
        }
        
        auto relationships = get_relationships_by_type(current_entity, type);
        for (const auto& rel : relationships) {
            std::string related_entity = rel.related_id;
            
            if (visited.find(related_entity) == visited.end()) {
                visited.insert(related_entity);
                result_entities.push_back(related_entity);
                queue.push({related_entity, depth + 1});
            }
        }
    }
    
    return result_entities;
}

RelationshipManager::Stats RelationshipManager::get_stats() const {
    Stats stats;
    
    stats.total_relationships = relationships_processed_.load();
    stats.relationships_inferred = relationships_inferred_.load();
    stats.memory_usage_bytes = entity_bloom_filter_->memory_usage() + 
                              relationship_bloom_filter_->memory_usage();
    
    // Calculate active relationships (approximate)
    // This would require scanning all wide rows in a real implementation
    stats.active_relationships = stats.total_relationships * 0.8; // Estimate
    
    // Calculate average confidence
    stats.avg_confidence = 0.75; // Estimate, would require actual calculation
    
    // Estimate bloom filter false positive rate
    stats.bloom_filter_fpr = 0.01; // Based on configuration
    
    return stats;
}

void RelationshipManager::compact_relationships() {
    // Iterate through all shards and compact wide rows that need it
    for (uint32_t shard_id = 0; shard_id < shard_manager_->num_shards(); ++shard_id) {
        auto* shard = shard_manager_->get_shard_by_id(shard_id);
        if (!shard) continue;
        
        // In a real implementation, we would scan for wide row keys
        // and compact those that need it
        // For now, this is a placeholder for the compaction logic
    }
}

void RelationshipManager::cleanup_expired_relationships() {
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    // Iterate through all shards and clean up expired relationships
    for (uint32_t shard_id = 0; shard_id < shard_manager_->num_shards(); ++shard_id) {
        auto* shard = shard_manager_->get_shard_by_id(shard_id);
        if (!shard) continue;
        
        // Scan and clean expired relationships
        // This would be implemented with a proper key scanning mechanism
    }
}

void RelationshipManager::optimize_memory_usage() {
    // Resize bloom filters if memory pressure is high
    resize_bloom_filters_if_needed();
    
    // Trigger compaction for large wide rows
    compact_relationships();
}

void RelationshipManager::store_relationship(const Relationship& relationship) {
    // Update bloom filters
    update_bloom_filters(relationship);
    
    // Store in wide row format
    std::string row_key = RelationshipWideRow::generate_row_key(
        relationship.entity_id, relationship.related_id);
    
    // Load existing wide row or create new one
    RelationshipWideRow wide_row = load_wide_row(relationship.entity_id);
    wide_row.add_relationship(relationship);
    
    // Store back to LSM tree
    store_wide_row(wide_row);
}

void RelationshipManager::update_bloom_filters(const Relationship& relationship) {
    // Add entities to bloom filter
    entity_bloom_filter_->add(relationship.entity_id);
    entity_bloom_filter_->add(relationship.related_id);
    
    // Add relationship to bloom filter
    std::string relationship_key = relationship.entity_id + ":" + relationship.related_id;
    relationship_bloom_filter_->add(relationship_key);
}

std::vector<Event> RelationshipManager::get_recent_events(
    const std::string& entity_id, uint64_t time_window_seconds) {
    
    std::vector<Event> recent_events;
    
    // In a real implementation, we would query the LSM tree for recent events
    // For now, return empty vector as placeholder
    // This would involve scanning event keys with timestamp ranges
    
    return recent_events;
}

RelationshipWideRow RelationshipManager::load_wide_row(const std::string& entity_id) {
    // Load wide row from LSM tree
    std::string row_key = "rel_row:" + entity_id;
    
    // Get the shard responsible for this entity
    auto* shard = shard_manager_->get_shard(row_key);
    if (!shard) {
        // Return empty wide row if shard not found
        return RelationshipWideRow(entity_id, nullptr);
    }
    
    // Query LSM tree for the wide row
    Value wide_row_data;
    bool found = shard->memtable().get(row_key, wide_row_data);
    
    if (!found) {
        // Try SSTables if not in memtable
        found = shard->sstable_manager().get(row_key, wide_row_data);
    }
    
    if (found) {
        return RelationshipWideRow::deserialize(wide_row_data, &shard->memory_pool());
    } else {
        // Return empty wide row
        return RelationshipWideRow(entity_id, &shard->memory_pool());
    }
}

void RelationshipManager::store_wide_row(const RelationshipWideRow& row) {
    std::string row_key = "rel_row:" + row.entity_id();
    std::string serialized_data = row.serialize();
    
    // Get the shard responsible for this entity
    auto* shard = shard_manager_->get_shard(row_key);
    if (!shard) {
        return; // Skip if shard not found
    }
    
    // Store in memtable (will be persisted to WAL and eventually SSTables)
    shard->memtable().put(row_key, serialized_data);
}

void RelationshipManager::check_memory_usage() {
    size_t current_usage = entity_bloom_filter_->memory_usage() + 
                           relationship_bloom_filter_->memory_usage();
    
    // If using more than 80% of allocated memory, trigger optimization
    if (current_usage > memory_limit_bytes_ * 0.8) {
        optimize_memory_usage();
    }
}

void RelationshipManager::resize_bloom_filters_if_needed() {
    size_t current_usage = entity_bloom_filter_->memory_usage() + 
                           relationship_bloom_filter_->memory_usage();
    
    if (current_usage > memory_limit_bytes_) {
        // Reduce bloom filter sizes to fit memory limit
        size_t new_limit = memory_limit_bytes_ / 2;
        entity_bloom_filter_->resize_for_memory_limit(new_limit);
        relationship_bloom_filter_->resize_for_memory_limit(new_limit);
    }
}

} // namespace nscfstore
