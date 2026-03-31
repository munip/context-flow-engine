#include <gtest/gtest.h>
#include "nscfstore/relationship.h"
#include "nscfstore/shard.h"
#include <chrono>

namespace nscfstore {

class RelationshipTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize test environment
        shard_manager_ = std::make_unique<ShardManager>(1); // Single shard for testing
        shard_manager_->start_all();
        
        relationship_manager_ = std::make_unique<RelationshipManager>(
            shard_manager_.get(), 1024 * 1024 * 1024); // 1GB for testing
    }
    
    void TearDown() override {
        relationship_manager_.reset();
        shard_manager_->stop_all();
        shard_manager_.reset();
    }
    
    std::unique_ptr<ShardManager> shard_manager_;
    std::unique_ptr<RelationshipManager> relationship_manager_;
};

TEST_F(RelationshipTest, BasicRelationshipCreation) {
    Event event;
    event.event_id = "test_event_1";
    event.entity_id = "entity_A";
    event.event_type = "interaction";
    event.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    event.payload = "entity_A interacts with entity_B";
    
    // Add explicit relationship
    Relationship explicit_rel;
    explicit_rel.entity_id = "entity_A";
    explicit_rel.related_id = "entity_B";
    explicit_rel.metadata.type = RelationshipType::INTERACTS_WITH;
    explicit_rel.metadata.inference_type = InferenceType::EXPLICIT;
    explicit_rel.metadata.confidence = 1.0;
    explicit_rel.metadata.created_at = event.timestamp;
    explicit_rel.metadata.source = "test_payload";
    
    event.explicit_relationships.push_back(explicit_rel);
    
    // Process the event
    relationship_manager_->process_event(event);
    
    // Verify relationship was stored
    auto relationships = relationship_manager_->get_relationships("entity_A");
    EXPECT_EQ(relationships.size(), 1);
    EXPECT_EQ(relationships[0].entity_id, "entity_A");
    EXPECT_EQ(relationships[0].related_id, "entity_B");
    EXPECT_EQ(relationships[0].metadata.type, RelationshipType::INTERACTS_WITH);
}

TEST_F(RelationshipTest, RelationshipInference) {
    Event event1;
    event1.event_id = "test_event_1";
    event1.entity_id = "entity_A";
    event1.event_type = "dependency";
    event1.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    event1.payload = "entity_A depends on entity_C";
    event1.features["type"] = "service";
    
    // Process first event
    relationship_manager_->process_event(event1);
    
    Event event2;
    event2.event_id = "test_event_2";
    event2.entity_id = "entity_B";
    event2.event_type = "interaction";
    event2.timestamp = event1.timestamp + 1000; // 1 second later
    event2.payload = "entity_B interacts with entity_A";
    event2.features["type"] = "service";
    
    // Process second event (should infer relationships)
    relationship_manager_->process_event(event2);
    
    // Check inferred relationships
    auto relationships_a = relationship_manager_->get_relationships("entity_A");
    auto relationships_b = relationship_manager_->get_relationships("entity_B");
    
    // Should have relationships from both events
    EXPECT_GT(relationships_a.size(), 0);
    EXPECT_GT(relationships_b.size(), 0);
    
    // Check for different relationship types
    bool has_dependency = false;
    bool has_interaction = false;
    
    for (const auto& rel : relationships_a) {
        if (rel.metadata.type == RelationshipType::DEPENDS_ON) {
            has_dependency = true;
        }
        if (rel.metadata.type == RelationshipType::INTERACTS_WITH) {
            has_interaction = true;
        }
    }
    
    EXPECT_TRUE(has_dependency);
    EXPECT_TRUE(has_interaction);
}

TEST_F(RelationshipTest, TemporalRelationshipInference) {
    auto base_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    // Create events with temporal proximity
    Event event1;
    event1.event_id = "temporal_event_1";
    event1.entity_id = "entity_A";
    event1.event_type = "start";
    event1.timestamp = base_time;
    event1.payload = "entity_A started";
    
    Event event2;
    event2.event_id = "temporal_event_2";
    event2.entity_id = "entity_B";
    event2.event_type = "start";
    event2.timestamp = base_time + 500; // 500ms later
    event2.payload = "entity_B started";
    
    // Process events
    relationship_manager_->process_event(event1);
    relationship_manager_->process_event(event2);
    
    // Check for temporal relationships
    auto relationships = relationship_manager_->get_relationships("entity_A");
    
    bool has_temporal_relationship = false;
    for (const auto& rel : relationships) {
        if (rel.metadata.inference_type == InferenceType::TEMPORAL) {
            has_temporal_relationship = true;
            EXPECT_GT(rel.metadata.confidence, 0.0);
            break;
        }
    }
    
    EXPECT_TRUE(has_temporal_relationship);
}

TEST_F(RelationshipTest, RelationshipSerialization) {
    Relationship rel;
    rel.entity_id = "test_entity";
    rel.related_id = "related_entity";
    rel.metadata.type = RelationshipType::CHILD_OF;
    rel.metadata.inference_type = InferenceType::SEMANTIC;
    rel.metadata.confidence = 0.85;
    rel.metadata.created_at = 1234567890;
    rel.metadata.expires_at = 0;
    rel.metadata.frequency = 5;
    rel.metadata.source = "test";
    rel.metadata.attributes["context"] = "test_context";
    
    // Test serialization
    std::string serialized = rel.serialize();
    EXPECT_FALSE(serialized.empty());
    
    // Test deserialization
    Relationship deserialized = Relationship::deserialize(serialized);
    
    EXPECT_EQ(rel.entity_id, deserialized.entity_id);
    EXPECT_EQ(rel.related_id, deserialized.related_id);
    EXPECT_EQ(rel.metadata.type, deserialized.metadata.type);
    EXPECT_EQ(rel.metadata.inference_type, deserialized.metadata.inference_type);
    EXPECT_DOUBLE_EQ(rel.metadata.confidence, deserialized.metadata.confidence);
    EXPECT_EQ(rel.metadata.created_at, deserialized.metadata.created_at);
    EXPECT_EQ(rel.metadata.expires_at, deserialized.metadata.expires_at);
    EXPECT_EQ(rel.metadata.frequency, deserialized.metadata.frequency);
    EXPECT_EQ(rel.metadata.source, deserialized.metadata.source);
    EXPECT_EQ(rel.metadata.attributes.at("context"), deserialized.metadata.attributes.at("context"));
}

TEST_F(RelationshipTest, EventSerialization) {
    Event event;
    event.event_id = "test_event";
    event.entity_id = "test_entity";
    event.event_type = "test_type";
    event.timestamp = 1234567890;
    event.payload = "test payload";
    event.features["key1"] = "value1";
    event.features["key2"] = "value2";
    
    Relationship rel;
    rel.entity_id = "test_entity";
    rel.related_id = "related_entity";
    rel.metadata.type = RelationshipType::REFERENCES;
    rel.metadata.inference_type = InferenceType::EXPLICIT;
    rel.metadata.confidence = 1.0;
    rel.metadata.created_at = event.timestamp;
    rel.metadata.source = "test";
    
    event.explicit_relationships.push_back(rel);
    
    // Test serialization
    std::string serialized = event.serialize();
    EXPECT_FALSE(serialized.empty());
    
    // Test deserialization
    Event deserialized = Event::deserialize(serialized);
    
    EXPECT_EQ(event.event_id, deserialized.event_id);
    EXPECT_EQ(event.entity_id, deserialized.entity_id);
    EXPECT_EQ(event.event_type, deserialized.event_type);
    EXPECT_EQ(event.timestamp, deserialized.timestamp);
    EXPECT_EQ(event.payload, deserialized.payload);
    EXPECT_EQ(event.features.size(), deserialized.features.size());
    EXPECT_EQ(event.explicit_relationships.size(), deserialized.explicit_relationships.size());
    
    if (!deserialized.explicit_relationships.empty()) {
        const auto& deserialized_rel = deserialized.explicit_relationships[0];
        EXPECT_EQ(rel.entity_id, deserialized_rel.entity_id);
        EXPECT_EQ(rel.related_id, deserialized_rel.related_id);
        EXPECT_EQ(rel.metadata.type, deserialized_rel.metadata.type);
    }
}

TEST_F(RelationshipTest, PartitionedBloomFilter) {
    PartitionedBloomFilter filter(1000, 0.01, 4);
    
    std::vector<std::string> test_items = {"item1", "item2", "item3", "item4", "item5"};
    
    // Add items to filter
    for (const auto& item : test_items) {
        filter.add(item);
    }
    
    // Test positive cases (should all be true, possibly with false positives)
    for (const auto& item : test_items) {
        EXPECT_TRUE(filter.might_contain(item));
    }
    
    // Test negative cases (should mostly be false, but may have false positives)
    std::vector<std::string> non_items = {"non_item1", "non_item2", "non_item3"};
    int false_positives = 0;
    
    for (const auto& item : non_items) {
        if (filter.might_contain(item)) {
            false_positives++;
        }
    }
    
    // False positive rate should be reasonable
    double false_positive_rate = static_cast<double>(false_positives) / non_items.size();
    EXPECT_LT(false_positive_rate, 0.1); // Should be less than 10%
    
    // Test memory usage
    size_t memory_usage = filter.memory_usage();
    EXPECT_GT(memory_usage, 0);
    
    // Test memory resize
    size_t original_memory = memory_usage;
    filter.resize_for_memory_limit(original_memory / 2);
    size_t resized_memory = filter.memory_usage();
    EXPECT_LE(resized_memory, original_memory / 2);
}

TEST_F(RelationshipTest, RelationshipWideRow) {
    // Create a mock memory pool for testing
    // In a real test, this would be properly initialized
    ShardMemoryPool* pool = nullptr;
    RelationshipWideRow row("test_entity", pool);
    
    // Add some relationships
    for (int i = 0; i < 5; ++i) {
        Relationship rel;
        rel.entity_id = "test_entity";
        rel.related_id = "related_entity_" + std::to_string(i);
        rel.metadata.type = RelationshipType::INTERACTS_WITH;
        rel.metadata.inference_type = InferenceType::EXPLICIT;
        rel.metadata.confidence = 0.8 + i * 0.04;
        rel.metadata.created_at = 1234567890 + i * 1000;
        rel.metadata.source = "test";
        
        row.add_relationship(rel);
    }
    
    // Test retrieval
    auto all_relationships = row.get_all_relationships();
    EXPECT_EQ(all_relationships.size(), 5);
    
    auto interact_relationships = row.get_relationships_by_type(RelationshipType::INTERACTS_WITH);
    EXPECT_EQ(interact_relationships.size(), 5);
    
    auto child_relationships = row.get_relationships_by_type(RelationshipType::CHILD_OF);
    EXPECT_EQ(child_relationships.size(), 0);
    
    // Test active relationships (all should be active since no expiration)
    Timestamp current_time = 1234567890 + 10000;
    auto active_relationships = row.get_active_relationships(current_time);
    EXPECT_EQ(active_relationships.size(), 5);
    
    // Test serialization
    std::string serialized = row.serialize();
    EXPECT_FALSE(serialized.empty());
    
    // Test size calculation
    size_t size_bytes = row.size_bytes();
    EXPECT_GT(size_bytes, 0);
}

TEST_F(RelationshipTest, RelationshipManagerStats) {
    Event event;
    event.event_id = "stats_test_event";
    event.entity_id = "stats_entity";
    event.event_type = "test";
    event.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    event.payload = "test payload for stats";
    
    // Process multiple events
    for (int i = 0; i < 10; ++i) {
        event.event_id = "stats_test_event_" + std::to_string(i);
        event.entity_id = "stats_entity_" + std::to_string(i);
        relationship_manager_->process_event(event);
    }
    
    // Get statistics
    auto stats = relationship_manager_->get_stats();
    
    EXPECT_GT(stats.total_relationships, 0);
    EXPECT_GE(stats.relationships_inferred, 0);
    EXPECT_GT(stats.memory_usage_bytes, 0);
    EXPECT_GT(stats.bloom_filter_fpr, 0.0);
}

TEST_F(RelationshipTest, ConnectedEntities) {
    // Create a chain of relationships: A -> B -> C -> D
    std::vector<std::string> entities = {"A", "B", "C", "D"};
    
    for (size_t i = 0; i < entities.size(); ++i) {
        Event event;
        event.event_id = "chain_event_" + std::to_string(i);
        event.entity_id = entities[i];
        event.event_type = "chain";
        event.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        event.payload = "chain event";
        
        if (i > 0) {
            Relationship rel;
            rel.entity_id = entities[i];
            rel.related_id = entities[i-1];
            rel.metadata.type = RelationshipType::DEPENDS_ON;
            rel.metadata.inference_type = InferenceType::EXPLICIT;
            rel.metadata.confidence = 1.0;
            rel.metadata.created_at = event.timestamp;
            rel.metadata.source = "test";
            
            event.explicit_relationships.push_back(rel);
        }
        
        relationship_manager_->process_event(event);
    }
    
    // Test connected entities with different depths
    auto connected_depth_1 = relationship_manager_->get_connected_entities("A", 1);
    EXPECT_EQ(connected_depth_1.size(), 1); // Should find B
    EXPECT_NE(std::find(connected_depth_1.begin(), connected_depth_1.end(), "B"), 
              connected_depth_1.end());
    
    auto connected_depth_2 = relationship_manager_->get_connected_entities("A", 2);
    EXPECT_EQ(connected_depth_2.size(), 2); // Should find B and C
    EXPECT_NE(std::find(connected_depth_2.begin(), connected_depth_2.end(), "B"), 
              connected_depth_2.end());
    EXPECT_NE(std::find(connected_depth_2.begin(), connected_depth_2.end(), "C"), 
              connected_depth_2.end());
    
    auto connected_depth_3 = relationship_manager_->get_connected_entities("A", 3);
    EXPECT_EQ(connected_depth_3.size(), 3); // Should find B, C, and D
}

} // namespace nscfstore
