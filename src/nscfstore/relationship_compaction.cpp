#include "nscfstore/relationship_compaction.h"
#include "nscfstore/shard.h"
#include <algorithm>
#include <chrono>
#include <unordered_map>

namespace nscfstore {

// RelationshipCompactionEngine Implementation
RelationshipCompactionEngine::RelationshipCompactionEngine(
    ShardManager* shard_manager, const CompactionConfig& config)
    : shard_manager_(shard_manager), config_(config) {
    
    // Initialize statistics
    stats_ = {};
}

RelationshipCompactionEngine::~RelationshipCompactionEngine() {
    stop();
}

void RelationshipCompactionEngine::start() {
    if (running_.load()) {
        return;
    }
    
    running_.store(true);
    
    // Start main compaction thread
    compaction_thread_ = std::thread(&RelationshipCompactionEngine::compaction_worker, this);
    
    // Start scheduler thread
    scheduler_thread_ = std::thread(&RelationshipCompactionEngine::scheduler_worker, this);
    
    // Start worker threads
    for (uint32_t i = 0; i < config_.max_concurrent_compactions; ++i) {
        worker_threads_.emplace_back(&RelationshipCompactionEngine::process_compaction_queue, this);
    }
}

void RelationshipCompactionEngine::stop() {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    
    // Wake up all threads
    queue_cv_.notify_all();
    compaction_cv_.notify_all();
    
    // Wait for threads to finish
    if (compaction_thread_.joinable()) {
        compaction_thread_.join();
    }
    
    if (scheduler_thread_.joinable()) {
        scheduler_thread_.join();
    }
    
    for (auto& worker : worker_threads_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    
    worker_threads_.clear();
}

void RelationshipCompactionEngine::wait_for_completion() {
    // Wait for all active compactions to complete
    std::unique_lock<std::mutex> lock(compaction_mutex_);
    compaction_cv_.wait(lock, [this] { return active_compactions_.load() == 0; });
}

void RelationshipCompactionEngine::compact_entity(const std::string& entity_id) {
    schedule_compaction(entity_id, 2.0); // High priority for manual compaction
}

void RelationshipCompactionEngine::compact_entities(const std::vector<std::string>& entity_ids) {
    for (const auto& entity_id : entity_ids) {
        schedule_compaction(entity_id, 1.5); // Medium-high priority
    }
}

void RelationshipCompactionEngine::schedule_compaction(const std::string& entity_id, double priority) {
    auto priority_info = analyze_entity(entity_id);
    priority_info.priority_score *= priority; // Apply multiplier
    
    std::lock_guard<std::mutex> lock(queue_mutex_);
    compaction_queue_.push(priority_info);
    queue_cv_.notify_one();
}

void RelationshipCompactionEngine::schedule_bulk_compaction() {
    auto candidates = find_candidates_for_compaction();
    
    for (const auto& entity_id : candidates) {
        auto priority_info = analyze_entity(entity_id);
        
        if (priority_info.priority_score >= config_.compaction_priority_threshold) {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            compaction_queue_.push(priority_info);
        }
    }
    
    queue_cv_.notify_all();
}

CompactionStats RelationshipCompactionEngine::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void RelationshipCompactionEngine::reset_stats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = {};
}

void RelationshipCompactionEngine::update_config(const CompactionConfig& config) {
    config_ = config;
    optimize_compaction_performance();
}

void RelationshipCompactionEngine::compaction_worker() {
    while (running_.load()) {
        try {
            // Schedule bulk compaction periodically
            schedule_bulk_compaction();
            
            // Sleep for compaction interval
            std::this_thread::sleep_for(std::chrono::seconds(config_.compaction_interval_seconds));
            
        } catch (const std::exception& e) {
            // Log error and continue
            // In a real implementation, would use proper logging
        }
    }
}

void RelationshipCompactionEngine::scheduler_worker() {
    while (running_.load()) {
        try {
            // Analyze system and schedule high-priority compactions
            check_compaction_limits();
            
            // Sleep for shorter interval for responsive scheduling
            std::this_thread::sleep_for(std::chrono::seconds(60));
            
        } catch (const std::exception& e) {
            // Log error and continue
        }
    }
}

void RelationshipCompactionEngine::process_compaction_queue() {
    while (running_.load()) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { 
            return !compaction_queue_.empty() || !running_.load(); 
        });
        
        if (!running_.load()) {
            break;
        }
        
        if (compaction_queue_.empty()) {
            continue;
        }
        
        auto priority = compaction_queue_.top();
        compaction_queue_.pop();
        lock.unlock();
        
        // Check compaction limits
        if (active_compactions_.load() >= config_.max_concurrent_compactions) {
            // Re-queue for later
            std::lock_guard<std::mutex> requeue_lock(queue_mutex_);
            compaction_queue_.push(priority);
            continue;
        }
        
        // Increment active compactions
        active_compactions_.fetch_add(1);
        
        try {
            auto start_time = std::chrono::high_resolution_clock::now();
            
            // Perform compaction
            bool success = compact_wide_row(priority.entity_id);
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            if (success) {
                // Update statistics
                update_compaction_stats(
                    priority.relationship_count,
                    priority.row_size_bytes / 4, // Estimate 25% space savings
                    duration.count(),
                    priority.expired_count,
                    0 // Duplicates would be counted during compaction
                );
            }
            
        } catch (const std::exception& e) {
            // Log error
        } finally {
            // Decrement active compactions
            active_compactions_.fetch_sub(1);
            compaction_cv_.notify_one();
        }
    }
}

bool RelationshipCompactionEngine::compact_wide_row(const std::string& entity_id) {
    // Load the wide row
    RelationshipWideRow row(entity_id, nullptr);
    
    // In a real implementation, we would load from LSM tree
    // For now, assume we have the row loaded
    
    if (!row.needs_compaction()) {
        return true; // No compaction needed
    }
    
    // Perform compaction
    auto compacted_row = compact_relationships(row);
    
    // Store the compacted row back
    // In a real implementation, this would be written to LSM tree
    
    return true;
}

RelationshipWideRow RelationshipCompactionEngine::compact_relationships(
    const RelationshipWideRow& row) {
    
    auto relationships = row.get_all_relationships();
    
    // Remove expired relationships
    relationships = remove_expired_relationships(relationships);
    
    // Remove duplicate relationships
    relationships = remove_duplicate_relationships(relationships);
    
    // Merge similar relationships
    relationships = merge_similar_relationships(relationships);
    
    // Apply frequency-based filtering
    relationships = apply_frequency_filter(relationships, row.entity_id_);
    
    // Create new compacted wide row
    RelationshipWideRow compacted_row(row.entity_id_, nullptr);
    for (const auto& rel : relationships) {
        compacted_row.add_relationship(rel);
    }
    
    return compacted_row;
}

std::vector<std::string> RelationshipCompactionEngine::find_candidates_for_compaction() {
    std::vector<std::string> candidates;
    
    // In a real implementation, we would scan all wide rows
    // For now, return empty vector as placeholder
    
    return candidates;
}

double RelationshipCompactionEngine::calculate_compaction_priority(const std::string& entity_id) {
    auto analysis = analyze_entity(entity_id);
    return analysis.priority_score;
}

CompactionPriority RelationshipCompactionEngine::analyze_entity(const std::string& entity_id) {
    CompactionPriority priority;
    priority.entity_id = entity_id;
    
    // In a real implementation, we would analyze the actual wide row
    // For now, provide default values
    
    priority.row_size_bytes = 0;
    priority.relationship_count = 0;
    priority.expired_count = 0;
    priority.last_access_time = 0;
    priority.access_frequency = 0.0;
    priority.priority_score = 0.0;
    
    return priority;
}

std::vector<Relationship> RelationshipCompactionEngine::remove_expired_relationships(
    const std::vector<Relationship>& relationships) {
    
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    std::vector<Relationship> active_relationships;
    
    for (const auto& rel : relationships) {
        if (!is_relationship_expired(rel)) {
            active_relationships.push_back(rel);
        }
    }
    
    return active_relationships;
}

std::vector<Relationship> RelationshipCompactionEngine::remove_duplicate_relationships(
    const std::vector<Relationship>& relationships) {
    
    std::vector<Relationship> unique_relationships;
    std::unordered_set<std::string> seen_keys;
    
    for (const auto& rel : relationships) {
        std::string key = rel.entity_id + ":" + rel.related_id + ":" + 
                         std::to_string(static_cast<int>(rel.metadata.type));
        
        if (seen_keys.find(key) == seen_keys.end()) {
            seen_keys.insert(key);
            unique_relationships.push_back(rel);
        }
    }
    
    return unique_relationships;
}

std::vector<Relationship> RelationshipCompactionEngine::merge_similar_relationships(
    const std::vector<Relationship>& relationships) {
    
    std::vector<Relationship> merged_relationships;
    std::unordered_map<std::string, std::vector<Relationship>> similar_groups;
    
    // Group similar relationships
    for (const auto& rel : relationships) {
        std::string key = rel.entity_id + ":" + rel.related_id;
        similar_groups[key].push_back(rel);
    }
    
    // Merge each group
    for (const auto& [key, group] : similar_groups) {
        if (group.size() == 1) {
            merged_relationships.push_back(group[0]);
        } else {
            // Merge relationships in the group
            Relationship merged = group[0];
            for (size_t i = 1; i < group.size(); ++i) {
                merged = merge_relationships(merged, group[i]);
            }
            merged_relationships.push_back(merged);
        }
    }
    
    return merged_relationships;
}

std::vector<Relationship> RelationshipCompactionEngine::apply_frequency_filter(
    const std::vector<Relationship>& relationships, const std::string& entity_id) {
    
    std::vector<Relationship> filtered_relationships;
    
    for (const auto& rel : relationships) {
        double access_freq = calculate_access_frequency(rel.related_id);
        
        if (access_freq >= config_.min_access_frequency) {
            filtered_relationships.push_back(rel);
        }
    }
    
    return filtered_relationships;
}

bool RelationshipCompactionEngine::should_compact(const std::string& entity_id) {
    auto analysis = analyze_entity(entity_id);
    
    switch (config_.strategy) {
        case CompactionStrategy::SIZE_BASED:
            return analysis.row_size_bytes > config_.max_row_size_bytes ||
                   analysis.relationship_count > config_.max_relationships_per_row;
            
        case CompactionStrategy::TIME_BASED: {
            Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            return (current_time - analysis.last_access_time) > 
                   config_.compaction_interval_seconds * 1000;
        }
        
        case CompactionStrategy::FREQUENCY_BASED:
            return analysis.access_frequency < config_.min_access_frequency;
            
        case CompactionStrategy::HYBRID:
            return calculate_compaction_priority(entity_id) >= 
                   config_.compaction_priority_threshold;
    }
    
    return false;
}

bool RelationshipCompactionEngine::is_relationship_expired(const Relationship& rel) {
    if (rel.metadata.expires_at == 0) {
        return false; // Permanent relationship
    }
    
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    return rel.metadata.expires_at <= current_time;
}

bool RelationshipCompactionEngine::are_relationships_similar(const Relationship& rel1, 
                                                           const Relationship& rel2) {
    return rel1.entity_id == rel2.entity_id &&
           rel1.related_id == rel2.related_id &&
           rel1.metadata.type == rel2.metadata.type;
}

Relationship RelationshipCompactionEngine::merge_relationships(const Relationship& rel1, 
                                                             const Relationship& rel2) {
    Relationship merged = rel1;
    
    // Use the higher confidence
    if (rel2.metadata.confidence > rel1.metadata.confidence) {
        merged.metadata.confidence = rel2.metadata.confidence;
    }
    
    // Use the most recent timestamp
    if (rel2.metadata.created_at > rel1.metadata.created_at) {
        merged.metadata.created_at = rel2.metadata.created_at;
    }
    
    // Sum frequencies
    merged.metadata.frequency = rel1.metadata.frequency + rel2.metadata.frequency;
    
    // Merge attributes
    for (const auto& [key, value] : rel2.metadata.attributes) {
        merged.metadata.attributes[key] = value;
    }
    
    // Update source to indicate merge
    merged.metadata.source = "merged:" + rel1.metadata.source + "," + rel2.metadata.source;
    
    return merged;
}

void RelationshipCompactionEngine::update_compaction_stats(
    uint64_t relationships_compacted, uint64_t bytes_saved, uint64_t compaction_time_ms,
    uint64_t expired_removed, uint64_t duplicates_removed) {
    
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    stats_.total_compactions++;
    stats_.relationships_compacted += relationships_compacted;
    stats_.bytes_saved += bytes_saved;
    
    // Update average compaction time
    uint64_t total_time = stats_.avg_compaction_time_ms * (stats_.total_compactions - 1) + compaction_time_ms;
    stats_.avg_compaction_time_ms = static_cast<double>(total_time) / stats_.total_compactions;
    
    stats_.expired_relationships_removed += expired_removed;
    stats_.duplicate_relationships_removed += duplicates_removed;
}

void RelationshipCompactionEngine::optimize_compaction_performance() {
    // Adjust thread pool size based on configuration
    size_t target_workers = config_.max_concurrent_compactions;
    
    if (worker_threads_.size() < target_workers) {
        // Add more workers
        for (size_t i = worker_threads_.size(); i < target_workers; ++i) {
            worker_threads_.emplace_back(&RelationshipCompactionEngine::process_compaction_queue, this);
        }
    } else if (worker_threads_.size() > target_workers) {
        // Signal excess workers to stop (simplified)
        // In a real implementation, would need proper worker shutdown
    }
}

void RelationshipCompactionEngine::check_compaction_limits() {
    // Check memory usage and adjust compaction aggressiveness
    // In a real implementation, would monitor system resources
    
    if (active_compactions_.load() > config_.max_concurrent_compactions * 0.8) {
        // Reduce compaction priority when system is busy
        // This would involve adjusting the priority calculation
    }
}

double RelationshipCompactionEngine::calculate_access_frequency(const std::string& entity_id) {
    // In a real implementation, would track access patterns
    // For now, return a default value
    return 0.5;
}

// SizeBasedCompactionPolicy Implementation
SizeBasedCompactionPolicy::SizeBasedCompactionPolicy(size_t max_size_bytes, size_t max_relationships)
    : max_size_bytes_(max_size_bytes), max_relationships_(max_relationships) {
}

bool SizeBasedCompactionPolicy::should_compact(const RelationshipWideRow& row) {
    return row.size_bytes() > max_size_bytes_ || 
           row.get_all_relationships().size() > max_relationships_;
}

double SizeBasedCompactionPolicy::get_priority(const RelationshipWideRow& row) {
    size_t size = row.size_bytes();
    size_t count = row.get_all_relationships().size();
    
    double size_priority = static_cast<double>(size) / max_size_bytes_;
    double count_priority = static_cast<double>(count) / max_relationships_;
    
    return std::max(size_priority, count_priority);
}

std::vector<Relationship> SizeBasedCompactionPolicy::compact(
    const std::vector<Relationship>& relationships) {
    
    // For size-based policy, primarily remove expired and duplicate relationships
    std::vector<Relationship> compacted;
    
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    std::unordered_set<std::string> seen;
    
    for (const auto& rel : relationships) {
        // Skip expired relationships
        if (rel.metadata.expires_at > 0 && rel.metadata.expires_at <= current_time) {
            continue;
        }
        
        // Skip duplicates
        std::string key = rel.entity_id + ":" + rel.related_id + ":" + 
                         std::to_string(static_cast<int>(rel.metadata.type));
        
        if (seen.find(key) != seen.end()) {
            continue;
        }
        
        seen.insert(key);
        compacted.push_back(rel);
    }
    
    return compacted;
}

// TimeBasedCompactionPolicy Implementation
TimeBasedCompactionPolicy::TimeBasedCompactionPolicy(uint64_t ttl_seconds, uint64_t interval_seconds)
    : ttl_seconds_(ttl_seconds), interval_seconds_(interval_seconds) {
}

bool TimeBasedCompactionPolicy::should_compact(const RelationshipWideRow& row) {
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    auto relationships = row.get_all_relationships();
    
    // Check if any relationships are expired
    for (const auto& rel : relationships) {
        if (rel.metadata.expires_at > 0 && rel.metadata.expires_at <= current_time) {
            return true;
        }
    }
    
    // Check if enough time has passed since last compaction
    // This would require tracking last compaction time
    return false;
}

double TimeBasedCompactionPolicy::get_priority(const RelationshipWideRow& row) {
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    auto relationships = row.get_all_relationships();
    
    size_t expired_count = 0;
    for (const auto& rel : relationships) {
        if (rel.metadata.expires_at > 0 && rel.metadata.expires_at <= current_time) {
            expired_count++;
        }
    }
    
    if (relationships.empty()) {
        return 0.0;
    }
    
    return static_cast<double>(expired_count) / relationships.size();
}

std::vector<Relationship> TimeBasedCompactionPolicy::compact(
    const std::vector<Relationship>& relationships) {
    
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    std::vector<Relationship> active_relationships;
    
    for (const auto& rel : relationships) {
        // Keep only non-expired relationships
        if (rel.metadata.expires_at == 0 || rel.metadata.expires_at > current_time) {
            active_relationships.push_back(rel);
        }
    }
    
    return active_relationships;
}

// FrequencyBasedCompactionPolicy Implementation
FrequencyBasedCompactionPolicy::FrequencyBasedCompactionPolicy(double min_frequency, uint64_t window_seconds)
    : min_frequency_(min_frequency), window_seconds_(window_seconds) {
}

bool FrequencyBasedCompactionPolicy::should_compact(const RelationshipWideRow& row) {
    auto relationships = row.get_all_relationships();
    
    for (const auto& rel : relationships) {
        double freq = calculate_access_frequency(rel.related_id);
        if (freq < min_frequency_) {
            return true;
        }
    }
    
    return false;
}

double FrequencyBasedCompactionPolicy::get_priority(const RelationshipWideRow& row) {
    auto relationships = row.get_all_relationships();
    
    double min_freq = 1.0;
    for (const auto& rel : relationships) {
        double freq = calculate_access_frequency(rel.related_id);
        min_freq = std::min(min_freq, freq);
    }
    
    return 1.0 - min_freq; // Higher priority for lower frequency
}

std::vector<Relationship> FrequencyBasedCompactionPolicy::compact(
    const std::vector<Relationship>& relationships) {
    
    std::vector<Relationship> filtered;
    
    for (const auto& rel : relationships) {
        double freq = calculate_access_frequency(rel.related_id);
        
        if (freq >= min_frequency_) {
            filtered.push_back(rel);
        }
    }
    
    return filtered;
}

double FrequencyBasedCompactionPolicy::calculate_access_frequency(const std::string& entity_id) {
    // In a real implementation, would track actual access patterns
    // For now, return a default value
    return 0.5;
}

// HybridCompactionPolicy Implementation
HybridCompactionPolicy::HybridCompactionPolicy(
    std::vector<std::unique_ptr<CompactionPolicy>> policies)
    : policies_(std::move(policies)) {
}

bool HybridCompactionPolicy::should_compact(const RelationshipWideRow& row) {
    for (const auto& policy : policies_) {
        if (policy->should_compact(row)) {
            return true;
        }
    }
    return false;
}

double HybridCompactionPolicy::get_priority(const RelationshipWideRow& row) {
    double max_priority = 0.0;
    
    for (const auto& policy : policies_) {
        max_priority = std::max(max_priority, policy->get_priority(row));
    }
    
    return max_priority;
}

std::vector<Relationship> HybridCompactionPolicy::compact(
    const std::vector<Relationship>& relationships) {
    
    // Apply all policies in sequence
    std::vector<Relationship> result = relationships;
    
    for (const auto& policy : policies_) {
        result = policy->compact(result);
    }
    
    return result;
}

} // namespace nscfstore
