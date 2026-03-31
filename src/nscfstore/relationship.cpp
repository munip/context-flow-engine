#include "nscfstore/relationship.h"
#include "nscfstore/shard.h"
#include <random>
#include <sstream>
#include <algorithm>
#include <regex>
#include <cmath>

namespace nscfstore {

// Relationship Serialization
std::string Relationship::serialize() const {
    std::ostringstream oss;
    oss << entity_id << "|" << related_id << "|"
        << static_cast<int>(metadata.type) << "|"
        << static_cast<int>(metadata.inference_type) << "|"
        << metadata.confidence << "|"
        << metadata.created_at << "|"
        << metadata.expires_at << "|"
        << metadata.frequency << "|"
        << metadata.source << "|";
    
    // Serialize attributes
    oss << "{";
    bool first = true;
    for (const auto& [key, value] : metadata.attributes) {
        if (!first) oss << ",";
        oss << key << "=" << value;
        first = false;
    }
    oss << "}";
    
    return oss.str();
}

Relationship Relationship::deserialize(const std::string& data) {
    Relationship rel;
    std::istringstream iss(data);
    std::string token;
    
    std::getline(iss, rel.entity_id, '|');
    std::getline(iss, rel.related_id, '|');
    
    std::getline(iss, token, '|');
    rel.metadata.type = static_cast<RelationshipType>(std::stoi(token));
    
    std::getline(iss, token, '|');
    rel.metadata.inference_type = static_cast<InferenceType>(std::stoi(token));
    
    std::getline(iss, token, '|');
    rel.metadata.confidence = std::stod(token);
    
    std::getline(iss, token, '|');
    rel.metadata.created_at = std::stoull(token);
    
    std::getline(iss, token, '|');
    rel.metadata.expires_at = std::stoull(token);
    
    std::getline(iss, token, '|');
    rel.metadata.frequency = std::stoull(token);
    
    std::getline(iss, rel.metadata.source, '|');
    
    // Parse attributes
    std::getline(iss, token);
    if (token.size() >= 2 && token.front() == '{' && token.back() == '}') {
        std::string attrs = token.substr(1, token.size() - 2);
        std::istringstream attr_stream(attrs);
        std::string attr_pair;
        
        while (std::getline(attr_stream, attr_pair, ',')) {
            size_t eq_pos = attr_pair.find('=');
            if (eq_pos != std::string::npos) {
                std::string key = attr_pair.substr(0, eq_pos);
                std::string value = attr_pair.substr(eq_pos + 1);
                rel.metadata.attributes[key] = value;
            }
        }
    }
    
    return rel;
}

// Event Serialization
std::string Event::serialize() const {
    std::ostringstream oss;
    oss << event_id << "|" << entity_id << "|" << event_type << "|"
        << timestamp << "|" << payload << "|";
    
    oss << "[";
    for (size_t i = 0; i < explicit_relationships.size(); ++i) {
        if (i > 0) oss << ";";
        oss << explicit_relationships[i].serialize();
    }
    oss << "]|";
    
    oss << "{";
    bool first = true;
    for (const auto& [key, value] : features) {
        if (!first) oss << ",";
        oss << key << "=" << value;
        first = false;
    }
    oss << "}";
    
    return oss.str();
}

Event Event::deserialize(const std::string& data) {
    Event event;
    std::istringstream iss(data);
    std::string token;
    
    std::getline(iss, event.event_id, '|');
    std::getline(iss, event.entity_id, '|');
    std::getline(iss, event.event_type, '|');
    
    std::getline(iss, token, '|');
    event.timestamp = std::stoull(token);
    
    std::getline(iss, event.payload, '|');
    
    // Parse relationships
    std::getline(iss, token, '|');
    if (token.size() >= 2 && token.front() == '[' && token.back() == ']') {
        std::string rels = token.substr(1, token.size() - 2);
        std::istringstream rel_stream(rels);
        std::string rel_str;
        
        while (std::getline(rel_stream, rel_str, ';')) {
            if (!rel_str.empty()) {
                event.explicit_relationships.push_back(Relationship::deserialize(rel_str));
            }
        }
    }
    
    // Parse features
    std::getline(iss, token);
    if (token.size() >= 2 && token.front() == '{' && token.back() == '}') {
        std::string feats = token.substr(1, token.size() - 2);
        std::istringstream feat_stream(feats);
        std::string feat_pair;
        
        while (std::getline(feat_stream, feat_pair, ',')) {
            size_t eq_pos = feat_pair.find('=');
            if (eq_pos != std::string::npos) {
                std::string key = feat_pair.substr(0, eq_pos);
                std::string value = feat_pair.substr(eq_pos + 1);
                event.features[key] = value;
            }
        }
    }
    
    return event;
}

// PartitionedBloomFilter Implementation
PartitionedBloomFilter::PartitionedBloomFilter(size_t expected_items, 
                                               double false_positive_rate,
                                               size_t num_partitions)
    : items_per_partition_(expected_items / num_partitions) {
    
    size_t bits_per_partition = static_cast<size_t>(
        -items_per_partition_ * std::log(false_positive_rate) / num_partitions / std::log(2)
    );
    
    // Ensure at least 64 bits per partition
    bits_per_partition = std::max(bits_per_partition, size_t(64));
    
    for (size_t i = 0; i < num_partitions; ++i) {
        Partition partition;
        partition.bits.resize(bits_per_partition / 64 + (bits_per_partition % 64 != 0), 0);
        partition.num_hash_functions = static_cast<size_t>(
            std::log(2) * bits_per_partition / items_per_partition_
        );
        partitions_.push_back(std::move(partition));
    }
}

void PartitionedBloomFilter::add(const std::string& item) {
    uint32_t partition_id = get_partition_id(item);
    auto hash_vals = hash_keys(item);
    
    partitions_[partition_id].add(item, hash_vals[partition_id % hash_vals.size()]);
}

bool PartitionedBloomFilter::might_contain(const std::string& item) const {
    uint32_t partition_id = get_partition_id(item);
    auto hash_vals = hash_keys(item);
    
    return partitions_[partition_id].might_contain(item, hash_vals[partition_id % hash_vals.size()]);
}

size_t PartitionedBloomFilter::memory_usage() const {
    size_t total = 0;
    for (const auto& partition : partitions_) {
        total += partition.bits.size() * sizeof(uint64_t);
    }
    return total;
}

void PartitionedBloomFilter::resize_for_memory_limit(size_t max_memory_bytes) {
    size_t current_usage = memory_usage();
    if (current_usage <= max_memory_bytes) {
        return;
    }
    
    // Reduce number of partitions to fit memory limit
    size_t new_num_partitions = (max_memory_bytes * partitions_.size()) / current_usage;
    new_num_partitions = std::max(new_num_partitions, size_t(1));
    
    if (new_num_partitions < partitions_.size()) {
        partitions_.resize(new_num_partitions);
        items_per_partition_ = items_per_partition_ * partitions_.size() / new_num_partitions;
    }
}

std::vector<char> PartitionedBloomFilter::serialize() const {
    std::vector<char> data;
    
    // Serialize metadata
    size_t num_partitions = partitions_.size();
    data.insert(data.end(), reinterpret_cast<const char*>(&num_partitions), 
                reinterpret_cast<const char*>(&num_partitions) + sizeof(num_partitions));
    
    data.insert(data.end(), reinterpret_cast<const char*>(&items_per_partition_),
                reinterpret_cast<const char*>(&items_per_partition_) + sizeof(items_per_partition_));
    
    // Serialize partitions
    for (const auto& partition : partitions_) {
        size_t num_hash_functions = partition.num_hash_functions;
        data.insert(data.end(), reinterpret_cast<const char*>(&num_hash_functions),
                    reinterpret_cast<const char*>(&num_hash_functions) + sizeof(num_hash_functions));
        
        size_t bits_size = partition.bits.size();
        data.insert(data.end(), reinterpret_cast<const char*>(&bits_size),
                    reinterpret_cast<const char*>(&bits_size) + sizeof(bits_size));
        
        data.insert(data.end(), reinterpret_cast<const char*>(partition.bits.data()),
                    reinterpret_cast<const char*>(partition.bits.data()) + 
                    bits_size * sizeof(uint64_t));
    }
    
    return data;
}

void PartitionedBloomFilter::deserialize(const std::vector<char>& data) {
    size_t offset = 0;
    
    // Deserialize metadata
    size_t num_partitions;
    std::memcpy(&num_partitions, data.data() + offset, sizeof(num_partitions));
    offset += sizeof(num_partitions);
    
    std::memcpy(&items_per_partition_, data.data() + offset, sizeof(items_per_partition_));
    offset += sizeof(items_per_partition_);
    
    // Deserialize partitions
    partitions_.clear();
    for (size_t i = 0; i < num_partitions; ++i) {
        Partition partition;
        
        std::memcpy(&partition.num_hash_functions, data.data() + offset, 
                    sizeof(partition.num_hash_functions));
        offset += sizeof(partition.num_hash_functions);
        
        size_t bits_size;
        std::memcpy(&bits_size, data.data() + offset, sizeof(bits_size));
        offset += sizeof(bits_size);
        
        partition.bits.resize(bits_size);
        std::memcpy(partition.bits.data(), data.data() + offset, 
                    bits_size * sizeof(uint64_t));
        offset += bits_size * sizeof(uint64_t);
        
        partitions_.push_back(std::move(partition));
    }
}

std::vector<uint32_t> PartitionedBloomFilter::hash_keys(const std::string& item) const {
    std::vector<uint32_t> hashes;
    
    // Multiple hash functions using different seeds
    for (size_t i = 0; i < 3; ++i) {
        uint32_t hash = 2166136261U;  // Proper 32-bit FNV offset basis
        uint32_t seed = i * 2654435761U;
        
        for (char c : item) {
            hash ^= static_cast<uint32_t>(c) + seed;
            hash *= 16777619U;  // Proper 32-bit FNV prime
        }
        hashes.push_back(hash);
    }
    
    return hashes;
}

uint32_t PartitionedBloomFilter::get_partition_id(const std::string& item) const {
    return hash_keys(item)[0] % partitions_.size();
}

void PartitionedBloomFilter::Partition::add(const std::string& item, uint32_t seed) {
    uint32_t hash = 2166136261U;  // Proper 32-bit FNV offset basis
    for (char c : item) {
        hash ^= static_cast<uint32_t>(c) + seed;
        hash *= 16777619U;  // Proper 32-bit FNV prime
    }
    
    for (size_t i = 0; i < num_hash_functions; ++i) {
        uint32_t bit_index = (hash + i * seed) % (bits.size() * 64);
        uint32_t word_index = bit_index / 64;
        uint32_t bit_offset = bit_index % 64;
        
        bits[word_index] |= (1ULL << bit_offset);
    }
}

bool PartitionedBloomFilter::Partition::might_contain(const std::string& item, uint32_t seed) const {
    uint32_t hash = 2166136261U;  // Proper 32-bit FNV offset basis
    for (char c : item) {
        hash ^= static_cast<uint32_t>(c) + seed;
        hash *= 16777619U;  // Proper 32-bit FNV prime
    }
    
    for (size_t i = 0; i < num_hash_functions; ++i) {
        uint32_t bit_index = (hash + i * seed) % (bits.size() * 64);
        uint32_t word_index = bit_index / 64;
        uint32_t bit_offset = bit_index % 64;
        
        if (!(bits[word_index] & (1ULL << bit_offset))) {
            return false;
        }
    }
    
    return true;
}

// RelationshipWideRow Implementation
RelationshipWideRow::RelationshipWideRow(const std::string& entity_id, ShardMemoryPool* pool)
    : entity_id_(entity_id), memory_pool_(pool) {
}

RelationshipWideRow::RelationshipWideRow(RelationshipWideRow&& other) noexcept
    : entity_id_(std::move(other.entity_id_)),
      relationships_(std::move(other.relationships_)),
      memory_pool_(other.memory_pool_) {
    other.memory_pool_ = nullptr;
}

RelationshipWideRow& RelationshipWideRow::operator=(RelationshipWideRow&& other) noexcept {
    if (this != &other) {
        std::lock_guard<std::mutex> lock1(relationships_mutex_);
        std::lock_guard<std::mutex> lock2(other.relationships_mutex_);
        
        entity_id_ = std::move(other.entity_id_);
        relationships_ = std::move(other.relationships_);
        memory_pool_ = other.memory_pool_;
        other.memory_pool_ = nullptr;
    }
    return *this;
}

void RelationshipWideRow::add_relationship(const Relationship& relationship) {
    std::lock_guard<std::mutex> lock(relationships_mutex_);
    relationships_.push_back(relationship);
    
    // Keep relationships sorted by timestamp for efficient queries
    sort_relationships_by_timestamp();
}

std::vector<Relationship> RelationshipWideRow::get_all_relationships() const {
    std::lock_guard<std::mutex> lock(relationships_mutex_);
    return relationships_;
}

std::vector<Relationship> RelationshipWideRow::get_relationships_by_type(RelationshipType type) const {
    std::lock_guard<std::mutex> lock(relationships_mutex_);
    std::vector<Relationship> result;
    
    for (const auto& rel : relationships_) {
        if (rel.metadata.type == type) {
            result.push_back(rel);
        }
    }
    
    return result;
}

std::vector<Relationship> RelationshipWideRow::get_active_relationships(Timestamp current_time) const {
    std::lock_guard<std::mutex> lock(relationships_mutex_);
    std::vector<Relationship> result;
    
    for (const auto& rel : relationships_) {
        if (rel.metadata.expires_at == 0 || rel.metadata.expires_at > current_time) {
            result.push_back(rel);
        }
    }
    
    return result;
}

std::string RelationshipWideRow::generate_row_key(const std::string& entity_id, 
                                                 const std::string& related_id) {
    return "rel:" + entity_id + ":" + related_id;
}

std::string RelationshipWideRow::serialize() const {
    std::lock_guard<std::mutex> lock(relationships_mutex_);
    std::ostringstream oss;
    
    oss << entity_id_ << "|";
    oss << relationships_.size() << "|";
    
    for (size_t i = 0; i < relationships_.size(); ++i) {
        if (i > 0) oss << ";";
        oss << relationships_[i].serialize();
    }
    
    return oss.str();
}

RelationshipWideRow RelationshipWideRow::deserialize(const std::string& data, 
                                                    ShardMemoryPool* pool) {
    std::istringstream iss(data);
    std::string token;
    
    std::getline(iss, token, '|');
    std::string entity_id = token;
    
    RelationshipWideRow row(entity_id, pool);
    
    std::getline(iss, token, '|');
    size_t num_relationships = std::stoull(token);
    
    std::getline(iss, token);
    if (!token.empty()) {
        std::istringstream rel_stream(token);
        std::string rel_str;
        
        for (size_t i = 0; i < num_relationships && std::getline(rel_stream, rel_str, ';'); ++i) {
            if (!rel_str.empty()) {
                row.relationships_.push_back(Relationship::deserialize(rel_str));
            }
        }
    }
    
    return row;
}

size_t RelationshipWideRow::size_bytes() const {
    std::lock_guard<std::mutex> lock(relationships_mutex_);
    size_t size = entity_id_.size() + sizeof(size_t); // entity_id + count
    
    for (const auto& rel : relationships_) {
        size += rel.serialize().size() + 1; // relationship + separator
    }
    
    return size;
}

bool RelationshipWideRow::needs_compaction() const {
    std::lock_guard<std::mutex> lock(relationships_mutex_);
    
    // Compact if we have more than 1000 relationships or if many are expired
    if (relationships_.size() > 1000) {
        return true;
    }
    
    Timestamp current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    size_t expired_count = 0;
    for (const auto& rel : relationships_) {
        if (rel.metadata.expires_at > 0 && rel.metadata.expires_at <= current_time) {
            expired_count++;
        }
    }
    
    return expired_count > relationships_.size() / 2; // More than half expired
}

void RelationshipWideRow::sort_relationships_by_timestamp() {
    std::sort(relationships_.begin(), relationships_.end(),
              [](const Relationship& a, const Relationship& b) {
                  return a.metadata.created_at > b.metadata.created_at;
              });
}

void RelationshipWideRow::remove_expired_relationships(Timestamp current_time) {
    relationships_.erase(
        std::remove_if(relationships_.begin(), relationships_.end(),
                       [current_time](const Relationship& rel) {
                           return rel.metadata.expires_at > 0 && 
                                  rel.metadata.expires_at <= current_time;
                       }),
        relationships_.end()
    );
}

} // namespace nscfstore
