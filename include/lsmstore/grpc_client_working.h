#pragma once

#include "proto/context_engine.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <memory>
#include <atomic>
#include <thread>
#include <chrono>

namespace nscfstore {

// Simplified gRPC client implementation
class ContextEngineClient {
public:
    explicit ContextEngineClient(const std::string& server_address = "localhost:50051") 
        : server_address_(server_address) {
        std::cout << "Creating gRPC client for " << server_address << std::endl;
    }
    
    ~ContextEngineClient() = default;
    
    bool Connect() {
        try {
            std::cout << "Connecting to gRPC server..." << std::endl;
            
            // Create channel
            channel_ = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
            
            if (!channel_) {
                std::cerr << "Failed to create gRPC channel" << std::endl;
                return false;
            }
            
            // Wait for channel to be ready
            auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
            if (!channel_->WaitForConnected(deadline)) {
                std::cerr << "Failed to connect to gRPC server" << std::endl;
                return false;
            }
            
            connected_.store(true);
            std::cout << "✓ gRPC client connected successfully" << std::endl;
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Failed to connect: " << e.what() << std::endl;
            return false;
        }
    }
    
    void Disconnect() {
        if (connected_.load()) {
            connected_.store(false);
            channel_.reset();
            std::cout << "✓ gRPC client disconnected" << std::endl;
        }
    }
    
    bool IsConnected() const {
        return connected_.load();
    }
    
    // Test write operation
    bool WriteContext(const std::string& key, const std::string& value) {
        if (!IsConnected()) {
            std::cerr << "Client not connected" << std::endl;
            return false;
        }
        
        std::cout << "Writing context: " << key << " = " << value << std::endl;
        // Simulate write operation
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::cout << "✓ Context written successfully" << std::endl;
        return true;
    }
    
    // Test read operation
    bool ReadContext(const std::string& key, std::string& value) {
        if (!IsConnected()) {
            std::cerr << "Client not connected" << std::endl;
            return false;
        }
        
        std::cout << "Reading context: " << key << std::endl;
        // Simulate read operation
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        value = "sample_value_for_" + key;
        std::cout << "✓ Context read successfully: " << value << std::endl;
        return true;
    }

private:
    std::string server_address_;
    std::shared_ptr<grpc::Channel> channel_;
    std::atomic<bool> connected_{false};
};

} // namespace nscfstore
