#include "nscfstore/relationship.h"
#include <algorithm>
#include <regex>
#include <cmath>
#include <unordered_set>
#include <sstream>
#include <iomanip>

namespace nscfstore {

// RelationshipInferenceEngine Implementation
RelationshipInferenceEngine::RelationshipInferenceEngine(ShardMemoryPool* pool)
    : memory_pool_(pool) {
}

std::vector<Relationship> RelationshipInferenceEngine::infer_relationships(
    const Event& event, const std::vector<Event>& recent_events) {
    
    std::vector<Relationship> inferred_relationships;
    
    // Temporal relationships
    auto temporal_rels = infer_temporal_relationships(event, recent_events);
    inferred_relationships.insert(inferred_relationships.end(),
                                 temporal_rels.begin(), temporal_rels.end());
    
    // Semantic relationships
    auto semantic_rels = infer_semantic_relationships(event);
    inferred_relationships.insert(inferred_relationships.end(),
                                 semantic_rels.begin(), semantic_rels.end());
    
    // Frequency-based relationships
    auto frequency_rels = infer_frequency_relationships(event, recent_events);
    inferred_relationships.insert(inferred_relationships.end(),
                                 frequency_rels.begin(), frequency_rels.end());
    
    // Filter by confidence threshold
    inferred_relationships.erase(
        std::remove_if(inferred_relationships.begin(), inferred_relationships.end(),
                       [this](const Relationship& rel) {
                           return rel.metadata.confidence < confidence_threshold_;
                       }),
        inferred_relationships.end()
    );
    
    return inferred_relationships;
}

std::vector<Relationship> RelationshipInferenceEngine::infer_temporal_relationships(
    const Event& event, const std::vector<Event>& recent_events) {
    
    std::vector<Relationship> relationships;
    
    for (const auto& other_event : recent_events) {
        if (other_event.entity_id == event.entity_id) {
            continue; // Skip self
        }
        
        double similarity = calculate_temporal_similarity(event, other_event);
        if (similarity < 0.3) { // Threshold for temporal similarity
            continue;
        }
        
        RelationshipType type = infer_relationship_type(event, other_event);
        
        Relationship rel;
        rel.entity_id = event.entity_id;
        rel.related_id = other_event.entity_id;
        rel.metadata.type = type;
        rel.metadata.inference_type = InferenceType::TEMPORAL;
        rel.metadata.confidence = similarity;
        rel.metadata.created_at = event.timestamp;
        rel.metadata.expires_at = event.timestamp + temporal_window_seconds_ * 1000;
        rel.metadata.frequency = 1;
        rel.metadata.source = "temporal_analysis";
        
        relationships.push_back(rel);
    }
    
    return relationships;
}

std::vector<Relationship> RelationshipInferenceEngine::infer_semantic_relationships(
    const Event& event) {
    
    std::vector<Relationship> relationships;
    
    // Extract semantic features from event
    std::string event_text = event.payload;
    std::string entity_type = event.features.count("type") ? event.features.at("type") : "";
    
    // Semantic patterns for different relationship types
    static const std::vector<std::pair<std::regex, RelationshipType>> semantic_patterns = {
        {std::regex(R"(\binteract(?:s|ed)?\s+with\b)", std::regex_constants::icase), RelationshipType::INTERACTS_WITH},
        {std::regex(R"(\bdepend(?:s|ed)?\s+on\b)", std::regex_constants::icase), RelationshipType::DEPENDS_ON},
        {std::regex(R"(\bchild\s+of\b)", std::regex_constants::icase), RelationshipType::CHILD_OF},
        {std::regex(R"(\bversion\s+\d+\b)", std::regex_constants::icase), RelationshipType::VERSION_MUTATION},
        {std::regex(R"(\bcause(?:s|d)?\b)", std::regex_constants::icase), RelationshipType::CAUSES},
        {std::regex(R"(\bprecede(?:s|d)?\b)", std::regex_constants::icase), RelationshipType::PRECEDES},
        {std::regex(R"(\bcontain(?:s|ed)?\b)", std::regex_constants::icase), RelationshipType::CONTAINS},
        {std::regex(R"(\breference(?:s|d)?\b)", std::regex_constants::icase), RelationshipType::REFERENCES}
    };
    
    // Check for semantic patterns
    for (const auto& [pattern, type] : semantic_patterns) {
        std::smatch match;
        if (std::regex_search(event_text, match, pattern)) {
            // Extract related entity from context
            std::string related_entity = extract_related_entity(event_text, match.position());
            
            if (!related_entity.empty() && related_entity != event.entity_id) {
                Relationship rel;
                rel.entity_id = event.entity_id;
                rel.related_id = related_entity;
                rel.metadata.type = type;
                rel.metadata.inference_type = InferenceType::SEMANTIC;
                rel.metadata.confidence = 0.8; // High confidence for explicit semantic patterns
                rel.metadata.created_at = event.timestamp;
                rel.metadata.expires_at = 0; // Permanent
                rel.metadata.frequency = 1;
                rel.metadata.source = "semantic_analysis";
                rel.metadata.attributes["pattern"] = match.str();
                
                relationships.push_back(rel);
            }
        }
    }
    
    return relationships;
}

std::vector<Relationship> RelationshipInferenceEngine::infer_frequency_relationships(
    const Event& event, const std::vector<Event>& recent_events) {
    
    std::vector<Relationship> relationships;
    
    // Count co-occurrences
    std::unordered_map<std::string, uint32_t> co_occurrence_counts;
    std::unordered_map<std::string, double> temporal_scores;
    
    for (const auto& other_event : recent_events) {
        if (other_event.entity_id == event.entity_id) {
            continue;
        }
        
        // Check for co-occurrence in similar time windows
        double temporal_score = calculate_temporal_similarity(event, other_event);
        if (temporal_score > 0.5) {
            co_occurrence_counts[other_event.entity_id]++;
            temporal_scores[other_event.entity_id] += temporal_score;
        }
    }
    
    // Infer relationships based on frequency patterns
    for (const auto& [entity_id, count] : co_occurrence_counts) {
        if (count >= 3) { // Minimum co-occurrence threshold
            double avg_temporal_score = temporal_scores[entity_id] / count;
            double confidence = std::min(0.9, count / 10.0 + avg_temporal_score / 2.0);
            
            Relationship rel;
            rel.entity_id = event.entity_id;
            rel.related_id = entity_id;
            rel.metadata.type = RelationshipType::INTERACTS_WITH; // Default to interaction
            rel.metadata.inference_type = InferenceType::FREQUENCY_BASED;
            rel.metadata.confidence = confidence;
            rel.metadata.created_at = event.timestamp;
            rel.metadata.expires_at = event.timestamp + temporal_window_seconds_ * 1000;
            rel.metadata.frequency = count;
            rel.metadata.source = "frequency_analysis";
            rel.metadata.attributes["co_occurrence_count"] = std::to_string(count);
            
            relationships.push_back(rel);
        }
    }
    
    return relationships;
}

double RelationshipInferenceEngine::calculate_temporal_similarity(
    const Event& event1, const Event& event2) {
    
    if (event1.timestamp == event2.timestamp) {
        return 1.0;
    }
    
    uint64_t time_diff = std::abs(static_cast<int64_t>(event1.timestamp) - 
                                 static_cast<int64_t>(event2.timestamp));
    
    // Exponential decay based on time difference
    double similarity = std::exp(-static_cast<double>(time_diff) / 
                                 (temporal_window_seconds_ * 1000.0 / 2.0));
    
    return similarity;
}

double RelationshipInferenceEngine::calculate_semantic_similarity(
    const std::string& entity1, const std::string& entity2) {
    
    if (entity1 == entity2) {
        return 1.0;
    }
    
    // Simple Jaccard similarity on character n-grams
    const size_t n = 3; // trigram similarity
    
    std::unordered_set<std::string> ngrams1, ngrams2;
    
    for (size_t i = 0; i + n <= entity1.length(); ++i) {
        ngrams1.insert(entity1.substr(i, n));
    }
    
    for (size_t i = 0; i + n <= entity2.length(); ++i) {
        ngrams2.insert(entity2.substr(i, n));
    }
    
    if (ngrams1.empty() || ngrams2.empty()) {
        return 0.0;
    }
    
    // Calculate Jaccard similarity
    std::unordered_set<std::string> intersection;
    for (const auto& ngram : ngrams1) {
        if (ngrams2.count(ngram)) {
            intersection.insert(ngram);
        }
    }
    
    std::unordered_set<std::string> union_set = ngrams1;
    for (const auto& ngram : ngrams2) {
        union_set.insert(ngram);
    }
    
    return static_cast<double>(intersection.size()) / union_set.size();
}

RelationshipType RelationshipInferenceEngine::infer_relationship_type(
    const Event& event1, const Event& event2) {
    
    // Analyze event types and features to infer relationship type
    
    // Check for version patterns
    if (is_version_related(event1, event2)) {
        return RelationshipType::VERSION_MUTATION;
    }
    
    // Check for dependency patterns
    if (is_dependency_related(event1, event2)) {
        return RelationshipType::DEPENDS_ON;
    }
    
    // Check for hierarchical patterns
    if (is_hierarchical_related(event1, event2)) {
        return RelationshipType::CHILD_OF;
    }
    
    // Check for causal patterns
    if (is_causal_related(event1, event2)) {
        return RelationshipType::CAUSES;
    }
    
    // Default to interaction
    return RelationshipType::INTERACTS_WITH;
}

std::string RelationshipInferenceEngine::extract_related_entity(
    const std::string& text, size_t match_position) {
    
    // Look for entity identifiers near the semantic pattern
    std::regex entity_pattern(R"([A-Za-z0-9_]{3,50})");
    std::sregex_iterator iter(text.begin(), text.end(), entity_pattern);
    std::sregex_iterator end;
    
    std::vector<std::pair<std::string, size_t>> candidates;
    
    for (; iter != end; ++iter) {
        std::string match = iter->str();
        size_t pos = iter->position();
        
        // Skip common words
        static const std::unordered_set<std::string> stop_words = {
            "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by"
        };
        
        if (stop_words.count(match)) {
            continue;
        }
        
        candidates.emplace_back(match, pos);
    }
    
    // Find the closest entity to the pattern match
    std::string closest_entity;
    size_t min_distance = std::string::npos;
    
    for (const auto& [entity, pos] : candidates) {
        size_t distance = std::abs(static_cast<ssize_t>(pos) - static_cast<ssize_t>(match_position));
        if (distance < min_distance) {
            min_distance = distance;
            closest_entity = entity;
        }
    }
    
    return closest_entity;
}

bool RelationshipInferenceEngine::is_version_related(const Event& event1, const Event& event2) {
    // Check if events represent different versions of the same entity
    if (event1.entity_id == event2.entity_id) {
        return false;
    }
    
    // Look for version patterns in entity IDs or features
    std::regex version_pattern(R"(.+[_-]v?\d+)");
    
    return std::regex_match(event1.entity_id, version_pattern) ||
           std::regex_match(event2.entity_id, version_pattern) ||
           event1.features.count("version") || event2.features.count("version");
}

bool RelationshipInferenceEngine::is_dependency_related(const Event& event1, const Event& event2) {
    // Check for dependency indicators in event types or payloads
    static const std::vector<std::string> dependency_keywords = {
        "require", "depend", "need", "import", "include", "use"
    };
    
    std::string combined_text = event1.event_type + " " + event1.payload + " " +
                                event2.event_type + " " + event2.payload;
    
    std::transform(combined_text.begin(), combined_text.end(), combined_text.begin(),
                   ::tolower);
    
    for (const auto& keyword : dependency_keywords) {
        if (combined_text.find(keyword) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

bool RelationshipInferenceEngine::is_hierarchical_related(const Event& event1, const Event& event2) {
    // Check for hierarchical relationships
    static const std::vector<std::string> hierarchical_keywords = {
        "parent", "child", "root", "leaf", "node", "branch", "tree"
    };
    
    std::string combined_text = event1.event_type + " " + event1.payload + " " +
                                event2.event_type + " " + event2.payload;
    
    std::transform(combined_text.begin(), combined_text.end(), combined_text.begin(),
                   ::tolower);
    
    for (const auto& keyword : hierarchical_keywords) {
        if (combined_text.find(keyword) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

bool RelationshipInferenceEngine::is_causal_related(const Event& event1, const Event& event2) {
    // Check for causal relationships based on temporal order and causal keywords
    
    // Event2 must occur after Event1 for causality
    if (event2.timestamp <= event1.timestamp) {
        return false;
    }
    
    static const std::vector<std::string> causal_keywords = {
        "cause", "trigger", "lead", "result", "effect", "because", "since", "due"
    };
    
    std::string combined_text = event1.event_type + " " + event1.payload + " " +
                                event2.event_type + " " + event2.payload;
    
    std::transform(combined_text.begin(), combined_text.end(), combined_text.begin(),
                   ::tolower);
    
    for (const auto& keyword : causal_keywords) {
        if (combined_text.find(keyword) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

} // namespace nscfstore
