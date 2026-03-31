#include "nscfstore/read_path.h"
#include <cstring>
#include <cmath>
#include <algorithm>

namespace nscfstore {

// OptimizedBloomFilter Implementation
OptimizedBloomFilter::OptimizedBloomFilter(size_t expected_items, double false_positive_rate) {
    // Calculate optimal number of bits and hash functions
    double ln2 = std::log(2);
    num_bits_ = static_cast<size_t>(
        -expected_items * std::log(false_positive_rate) / (ln2 * ln2)
    );
    
    // Ensure minimum size and cache line alignment
    num_bits_ = std::max(num_bits_, size_t(64));
    num_bits_ = ((num_bits_ + 63) / 64) * 64; // Align to 64-bit boundary
    
    num_hash_functions_ = static_cast<size_t>(
        std::log(2) * num_bits_ / expected_items
    );
    
    num_hash_functions_ = std::max(num_hash_functions_, size_t(1));
    num_hash_functions_ = std::min(num_hash_functions_, size_t(8)); // Limit to 8 hash functions
    
    // Allocate bit array
    size_t num_words = (num_bits_ + 63) / 64;
    bits_.resize(num_words, 0);
}

OptimizedBloomFilter::~OptimizedBloomFilter() = default;

void OptimizedBloomFilter::add(const Key& key) {
    auto hashes = hash_keys(key);
    
    for (size_t i = 0; i < num_hash_functions_; ++i) {
        size_t bit_index = hashes[i] % num_bits_;
        set_bit(bit_index);
    }
    
    num_items_.fetch_add(1);
}

bool OptimizedBloomFilter::might_contain(const Key& key) const {
    auto hashes = hash_keys(key);
    
    for (size_t i = 0; i < num_hash_functions_; ++i) {
        size_t bit_index = hashes[i] % num_bits_;
        if (!get_bit(bit_index)) {
            return false;
        }
    }
    
    return true;
}

std::vector<char> OptimizedBloomFilter::serialize() const {
    // Serialize header and bit array
    struct Header {
        uint64_t magic;
        uint32_t version;
        uint64_t num_bits;
        uint32_t num_hash_functions;
        uint64_t num_items;
        uint32_t checksum;
    } header;
    
    header.magic = 0x424C4F4F; // "BLOO"
    header.version = 1;
    header.num_bits = num_bits_;
    header.num_hash_functions = num_hash_functions_;
    header.num_items = num_items_.load();
    header.checksum = 0; // TODO: Calculate checksum
    
    // Calculate total size
    size_t header_size = sizeof(Header);
    size_t bits_size = bits_.size() * sizeof(uint64_t);
    std::vector<char> data(header_size + bits_size);
    
    // Copy header
    std::memcpy(data.data(), &header, header_size);
    
    // Copy bit array
    std::memcpy(data.data() + header_size, bits_.data(), bits_size);
    
    return data;
}

bool OptimizedBloomFilter::deserialize(const std::vector<char>& data) {
    if (data.size() < sizeof(Header)) {
        return false;
    }
    
    // Parse header
    const Header* header = reinterpret_cast<const Header*>(data.data());
    
    if (header->magic != 0x424C4F4F || header->version != 1) {
        return false;
    }
    
    num_bits_ = header->num_bits;
    num_hash_functions_ = header->num_hash_functions;
    num_items_.store(header->num_items);
    
    // Parse bit array
    size_t header_size = sizeof(Header);
    size_t bits_size = header->num_bits / 8 + ((header->num_bits % 8) != 0);
    
    if (data.size() < header_size + bits_size) {
        return false;
    }
    
    size_t num_words = (num_bits_ + 63) / 64;
    bits_.resize(num_words);
    std::memcpy(bits_.data(), data.data() + header_size, 
                std::min(bits_size, num_words * sizeof(uint64_t)));
    
    return true;
}

size_t OptimizedBloomFilter::memory_usage() const {
    return bits_.size() * sizeof(uint64_t) + sizeof(*this);
}

std::vector<uint64_t> OptimizedBloomFilter::hash_keys(const Key& key) const {
    std::vector<uint64_t> hashes;
    hashes.reserve(num_hash_functions_);
    
    // Use XXHash for fast, high-quality hashing
    uint64_t hash1 = XXH64(key.data(), key.size(), 0);
    uint64_t hash2 = XXH64(key.data(), key.size(), hash1);
    
    // Generate multiple hash functions using double hashing technique
    for (size_t i = 0; i < num_hash_functions_; ++i) {
        uint64_t combined_hash = hash1 + i * hash2;
        hashes.push_back(combined_hash);
    }
    
    return hashes;
}

void OptimizedBloomFilter::set_bit(size_t index) {
    size_t word_index = index / 64;
    size_t bit_offset = index % 64;
    
    bits_[word_index] |= (1ULL << bit_offset);
}

bool OptimizedBloomFilter::get_bit(size_t index) const {
    size_t word_index = index / 64;
    size_t bit_offset = index % 64;
    
    return (bits_[word_index] & (1ULL << bit_offset)) != 0;
}

} // namespace nscfstore
