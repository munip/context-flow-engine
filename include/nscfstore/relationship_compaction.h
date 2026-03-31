#pragma once

#include "relationship.h"
#include <memory>
#include <vector>
#include <queue>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace nscfstore {

// Compaction strategies for relationship wide rows
enum class CompactionStrategy {
    SIZE_BASED,        // Compact when row exceeds size threshold
    TIME_BASED,        // Compact based on time intervals
    FREQUENCY_BASED,   // Compact based on access frequency
    HYBRID            // Combination of multiple strategies
};

// Compaction priority based on various factors
struct CompactionPriority {
    std::string entity_id;
    double priority_score;
    size_t row_size_bytes;
    uint64_t relationship_count;
    uint64_t expired_count;
    uint64_t last_access_time;
    double access_frequency;
    
    bool operator<(const CompactionPriority& other) const {
        return priority_score < other.priority_score; // Min-heap
    }
};

// Relationship compaction statistics
struct CompactionStats {
    uint64_t total_compactions;
    uint64_t relationships_compacted;
    uint64_t bytes_saved;
    double avg_compaction_time_ms;
    uint64_t expired_relationships_removed;
    uint64_t duplicate_relationships_removed;
};

// Compaction configuration
struct CompactionConfig {
    CompactionStrategy strategy = CompactionStrategy::HYBRID;
    
    // Size-based thresholds
    size_t max_row_size_bytes = 1024 * 1024;  // 1MB per wide row
    size_t max_relationships_per_row = 10000;
    
    // Time-based thresholds
    uint64_t compaction_interval_seconds = 3600;  // 1 hour
    uint64_t relationship_ttl_seconds = 86400 * 30; // 30 days
    
    // Frequency-based thresholds
    double min_access_frequency = 0.1;  // Minimum access frequency to keep
    uint64_t access_window_seconds = 86400; // 24 hour window for frequency calculation
    
    // Performance tuning
    uint32_t max_concurrent_compactions = 2;
    uint32_t compaction_batch_size = 100;
    double compaction_priority_threshold = 0.5;
};

// Relationship compaction engine
class RelationshipCompactionEngine {
public:
    explicit RelationshipCompactionEngine(ShardManager* shard_manager,
                                         const CompactionConfig& config = CompactionConfig{});
    ~RelationshipCompactionEngine();
    
    // Start/stop compaction engine
    void start();
    void stop();
    void wait_for_completion();
    
    // Manual compaction
    void compact_entity(const std::string& entity_id);
    void compact_entities(const std::vector<std::string>& entity_ids);
    
    // Compaction scheduling
    void schedule_compaction(const std::string& entity_id, double priority = 1.0);
    void schedule_bulk_compaction();
    
    // Statistics and monitoring
    CompactionStats get_stats() const;
    void reset_stats();
    
    // Configuration
    void update_config(const CompactionConfig& config);
    CompactionConfig get_config() const { return config_; }
    
private:
    ShardManager* shard_manager_;
    CompactionConfig config_;
    
    // Compaction state
    std::atomic<bool> running_{false};
    std::thread compaction_thread_;
    std::thread scheduler_thread_;
    
    // Compaction queue (priority queue for high-priority entities)
    std::priority_queue<CompactionPriority> compaction_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    CompactionStats stats_;
    
    // Worker thread management
    std::vector<std::thread> worker_threads_;
    std::atomic<uint32_t> active_compactions_{0};
    std::mutex compaction_mutex_;
    std::condition_variable compaction_cv_;
    
    // Main methods
    void compaction_worker();
    void scheduler_worker();
    void process_compaction_queue();
    
    // Compaction logic
    bool compact_wide_row(const std::string& entity_id);
    RelationshipWideRow compact_relationships(const RelationshipWideRow& row);
    
    // Compaction strategies
    std::vector<std::string> find_candidates_for_compaction();
    double calculate_compaction_priority(const std::string& entity_id);
    CompactionPriority analyze_entity(const std::string& entity_id);
    
    // Specific compaction operations
    std::vector<Relationship> remove_expired_relationships(
        const std::vector<Relationship>& relationships);
    std::vector<Relationship> remove_duplicate_relationships(
        const std::vector<Relationship>& relationships);
    std::vector<Relationship> merge_similar_relationships(
        const std::vector<Relationship>& relationships);
    std::vector<Relationship> apply_frequency_filter(
        const std::vector<Relationship>& relationships, const std::string& entity_id);
    
    // Utility methods
    bool should_compact(const std::string& entity_id);
    bool is_relationship_expired(const Relationship& rel);
    bool are_relationships_similar(const Relationship& rel1, const Relationship& rel2);
    Relationship merge_relationships(const Relationship& rel1, const Relationship& rel2);
    
    // Statistics tracking
    void update_compaction_stats(uint64_t relationships_compacted,
                                uint64_t bytes_saved,
                                uint64_t compaction_time_ms,
                                uint64_t expired_removed,
                                uint64_t duplicates_removed);
    
    // Memory and performance optimization
    void optimize_compaction_performance();
    void check_compaction_limits();
};

// Compaction policy for different relationship types
class CompactionPolicy {
public:
    virtual ~CompactionPolicy() = default;
    
    virtual bool should_compact(const RelationshipWideRow& row) = 0;
    virtual double get_priority(const RelationshipWideRow& row) = 0;
    virtual std::vector<Relationship> compact(const std::vector<Relationship>& relationships) = 0;
};

// Size-based compaction policy
class SizeBasedCompactionPolicy : public CompactionPolicy {
public:
    explicit SizeBasedCompactionPolicy(size_t max_size_bytes, size_t max_relationships);
    
    bool should_compact(const RelationshipWideRow& row) override;
    double get_priority(const RelationshipWideRow& row) override;
    std::vector<Relationship> compact(const std::vector<Relationship>& relationships) override;
    
private:
    size_t max_size_bytes_;
    size_t max_relationships_;
};

// Time-based compaction policy
class TimeBasedCompactionPolicy : public CompactionPolicy {
public:
    explicit TimeBasedCompactionPolicy(uint64_t ttl_seconds, uint64_t interval_seconds);
    
    bool should_compact(const RelationshipWideRow& row) override;
    double get_priority(const RelationshipWideRow& row) override;
    std::vector<Relationship> compact(const std::vector<Relationship>& relationships) override;
    
private:
    uint64_t ttl_seconds_;
    uint64_t interval_seconds_;
};

// Frequency-based compaction policy
class FrequencyBasedCompactionPolicy : public CompactionPolicy {
public:
    explicit FrequencyBasedCompactionPolicy(double min_frequency, uint64_t window_seconds);
    
    bool should_compact(const RelationshipWideRow& row) override;
    double get_priority(const RelationshipWideRow& row) override;
    std::vector<Relationship> compact(const std::vector<Relationship>& relationships) override;
    
private:
    double min_frequency_;
    uint64_t window_seconds_;
    
    double calculate_access_frequency(const std::string& entity_id);
};

// Hybrid compaction policy combining multiple strategies
class HybridCompactionPolicy : public CompactionPolicy {
public:
    HybridCompactionPolicy(std::vector<std::unique_ptr<CompactionPolicy>> policies);
    
    bool should_compact(const RelationshipWideRow& row) override;
    double get_priority(const RelationshipWideRow& row) override;
    std::vector<Relationship> compact(const std::vector<Relationship>& relationships) override;
    
private:
    std::vector<std::unique_ptr<CompactionPolicy>> policies_;
};

} // namespace nscfstore
