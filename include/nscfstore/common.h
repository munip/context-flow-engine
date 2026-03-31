#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <unordered_map>
#include <chrono>

namespace nscfstore {

using Key = std::string;
using Value = std::string;
using Timestamp = uint64_t;

enum class OperationType {
    GET,
    PUT,
    DELETE,
    SCAN
};

struct Request {
    OperationType type;
    Key key;
    Value value;
    uint32_t client_id;
    uint64_t request_id;
    std::chrono::steady_clock::time_point start_time;
};

struct Response {
    bool success;
    Value value;
    std::string error;
    uint64_t request_id;
    std::chrono::steady_clock::time_point completion_time;
};

constexpr uint32_t MAX_SHARDS = 128;
constexpr uint32_t CACHE_LINE_SIZE = 64;
constexpr uint32_t MAX_SKIP_LIST_LEVELS = 16;

// Align structures to cache lines to avoid false sharing
template<typename T>
struct alignas(CACHE_LINE_SIZE) CacheLineAligned {
    T value;
};

// Utility functions
inline uint64_t hash_key(const Key& key) {
    uint64_t hash = 14695981039346656037ULL;
    for (char c : key) {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}

inline uint32_t get_shard_id(const Key& key, uint32_t num_shards) {
    return hash_key(key) % num_shards;
}

} // namespace nscfstore
