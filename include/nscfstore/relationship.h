#pragma once

#include "common.h"
#include "memory_pool.h"
#include "shard.h"
#include "memtable.h"
#include "sstable.h"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <atomic>
#include <chrono>

namespace nscfstore {

// Event and relationship types
enum class RelationshipType {
    INTERACTS_WITH,     // A interacts with B
    DEPENDS_ON,         // A depends on C
    CHILD_OF,           // A is child of D
    VERSION_MUTATION,   // Version mutation of A
    TEMPORAL_SELF,      // Temporal relationship with self
    CAUSES,             // A causes B
    PRECEDES,           // A precedes B
    CONTAINS,           // A contains B
    REFERENCES          // A references B
};

enum class InferenceType {
    EXPLICIT,           // Explicitly provided in event
    TEMPORAL,           // Inferred from temporal patterns
    SEMANTIC,           // Inferred from semantic analysis
    FREQUENCY_BASED,    // Inferred from frequency patterns
    CONTEXTUAL          // Inferred from context
};

struct RelationshipMetadata {
    RelationshipType type;
    InferenceType inference_type;
    double confidence;           // 0.0 to 1.0
    Timestamp created_at;
    Timestamp expires_at;        // 0 if permanent
    uint32_t frequency;          // How often this relationship occurs
    std::string source;          // Source of relationship (e.g., "event_payload", "temporal_analysis")
    std::unordered_map<std::string, std::string> attributes; // Additional metadata
};

struct Relationship {
    std::string entity_id;
    std::string related_id;
    RelationshipMetadata metadata;
    
    // Serialization helpers
    std::string serialize() const;
    static Relationship deserialize(const std::string& data);
};

// Event structure for relationship processing
struct Event {
    std::string event_id;
    std::string entity_id;
    std::string event_type;
    Timestamp timestamp;
    std::string payload;
    
    // Pre-existing relationships in event payload
    std::vector<Relationship> explicit_relationships;
    
    // Entity features for inference
    std::unordered_map<std::string, std::string> features;
    
    std::string serialize() const;
    static Event deserialize(const std::string& data);
};

// Partitioned Bloom Filter for memory optimization
class PartitionedBloomFilter {
public:
    explicit PartitionedBloomFilter(size_t expected_items, 
                                   double false_positive_rate = 0.01,
                                   size_t num_partitions = 16);
    
    void add(const std::string& item);
    bool might_contain(const std::string& item) const;
    
    // Memory optimization for 16GB RAM
    size_t memory_usage() const;
    void resize_for_memory_limit(size_t max_memory_bytes);
    
    // Serialization for persistence
    std::vector<char> serialize() const;
    void deserialize(const std::vector<char>& data);
    
private:
    struct Partition {
        std::vector<uint64_t> bits;
        size_t num_hash_functions;
        
        void add(const std::string& item, uint32_t seed);
        bool might_contain(const std::string& item, uint32_t seed) const;
    };
    
    std::vector<Partition> partitions_;
    size_t items_per_partition_;
    
    std::vector<uint32_t> hash_keys(const std::string& item) const;
    uint32_t get_partition_id(const std::string& item) const;
};

// Wide Row implementation for relationships
class RelationshipWideRow {
public:
    explicit RelationshipWideRow(const std::string& entity_id, ShardMemoryPool* pool);
    ~RelationshipWideRow() = default;
    
    // Move constructor and assignment
    RelationshipWideRow(RelationshipWideRow&& other) noexcept;
    RelationshipWideRow& operator=(RelationshipWideRow&& other) noexcept;
    
    // Delete copy constructor and assignment
    RelationshipWideRow(const RelationshipWideRow&) = delete;
    RelationshipWideRow& operator=(const RelationshipWideRow&) = delete;
    
    // Append-only relationship updates
    void add_relationship(const Relationship& relationship);
    
    // Query relationships
    std::vector<Relationship> get_all_relationships() const;
    std::vector<Relationship> get_relationships_by_type(RelationshipType type) const;
    std::vector<Relationship> get_active_relationships(Timestamp current_time) const;
    
    // Wide row key generation
    static std::string generate_row_key(const std::string& entity_id, 
                                       const std::string& related_id);
    
    // Serialization for LSM storage
    std::string serialize() const;
    static RelationshipWideRow deserialize(const std::string& data, 
                                         ShardMemoryPool* pool);
    
    // Memory management
    size_t size_bytes() const;
    bool needs_compaction() const;
    
    // Accessors
    const std::string& entity_id() const { return entity_id_; }
    const std::vector<Relationship>& relationships() const { return relationships_; }
    
private:
    std::string entity_id_;
    std::vector<Relationship> relationships_;
    ShardMemoryPool* memory_pool_;
    
    mutable std::mutex relationships_mutex_;
    
    void sort_relationships_by_timestamp();
    void remove_expired_relationships(Timestamp current_time);
};

// Relationship inference engine
class RelationshipInferenceEngine {
public:
    explicit RelationshipInferenceEngine(ShardMemoryPool* pool);
    
    // Main inference function
    std::vector<Relationship> infer_relationships(const Event& event, 
                                                 const std::vector<Event>& recent_events);
    
    // Specific inference strategies
    std::vector<Relationship> infer_temporal_relationships(const Event& event,
                                                           const std::vector<Event>& recent_events);
    
    std::vector<Relationship> infer_semantic_relationships(const Event& event);
    
    std::vector<Relationship> infer_frequency_relationships(const Event& event,
                                                           const std::vector<Event>& recent_events);
    
    // Configuration
    void set_confidence_threshold(double threshold) { confidence_threshold_ = threshold; }
    void set_temporal_window_seconds(uint64_t seconds) { temporal_window_seconds_ = seconds; }
    
private:
    ShardMemoryPool* memory_pool_;
    double confidence_threshold_ = 0.7;
    uint64_t temporal_window_seconds_ = 3600; // 1 hour
    
    // Inference helpers
    double calculate_temporal_similarity(const Event& event1, const Event& event2);
    double calculate_semantic_similarity(const std::string& entity1, const std::string& entity2);
    RelationshipType infer_relationship_type(const Event& event1, const Event& event2);
    
    // Additional helper methods
    std::string extract_related_entity(const std::string& text, size_t match_position);
    bool is_version_related(const Event& event1, const Event& event2);
    bool is_dependency_related(const Event& event1, const Event& event2);
    bool is_hierarchical_related(const Event& event1, const Event& event2);
    bool is_causal_related(const Event& event1, const Event& event2);
};

// Main Relationship Manager
class RelationshipManager {
public:
    explicit RelationshipManager(ShardManager* shard_manager, 
                                 size_t memory_limit_bytes = 16ULL * 1024 * 1024 * 1024);
    ~RelationshipManager();
    
    // Main interface
    void process_event(const Event& event);
    
    // Relationship queries
    std::vector<Relationship> get_relationships(const std::string& entity_id);
    std::vector<Relationship> get_active_relationships(const std::string& entity_id);
    std::vector<Relationship> get_relationships_by_type(const std::string& entity_id,
                                                        RelationshipType type);
    
    // Entity graph traversal
    std::vector<std::string> get_connected_entities(const std::string& entity_id,
                                                  int max_depth = 2);
    std::vector<std::string> get_entities_by_relationship(const std::string& entity_id,
                                                          RelationshipType type,
                                                          int max_depth = 1);
    
    // Statistics and monitoring
    struct Stats {
        uint64_t total_relationships;
        uint64_t active_relationships;
        uint64_t entities_processed;
        uint64_t relationships_inferred;
        double avg_confidence;
        size_t memory_usage_bytes;
        double bloom_filter_fpr;
    };
    
    Stats get_stats() const;
    
    // Maintenance operations
    void compact_relationships();
    void cleanup_expired_relationships();
    void optimize_memory_usage();
    
private:
    ShardManager* shard_manager_;
    std::unique_ptr<RelationshipInferenceEngine> inference_engine_;
    std::unique_ptr<PartitionedBloomFilter> entity_bloom_filter_;
    std::unique_ptr<PartitionedBloomFilter> relationship_bloom_filter_;
    
    size_t memory_limit_bytes_;
    uint64_t temporal_window_seconds_;
    std::atomic<uint64_t> relationships_processed_{0};
    std::atomic<uint64_t> relationships_inferred_{0};
    
    // Internal methods
    void store_relationship(const Relationship& relationship);
    void update_bloom_filters(const Relationship& relationship);
    std::vector<Event> get_recent_events(const std::string& entity_id, uint64_t time_window_seconds);
    
    // Wide row operations
    RelationshipWideRow load_wide_row(const std::string& entity_id);
    void store_wide_row(const RelationshipWideRow& row);
    
    // Memory management
    void check_memory_usage();
    void resize_bloom_filters_if_needed();
};

} // namespace nscfstore
