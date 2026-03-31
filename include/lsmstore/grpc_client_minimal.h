#pragma once

#include <iostream>
#include <string>
#include <memory>

namespace nscfstore {

// Simplified gRPC client for testing
class ContextEngineClient {
public:
    explicit ContextEngineClient(const std::string& server_address = "localhost:50051") {
        std::cout << "Mock gRPC client created for " << server_address << std::endl;
    }
    
    ~ContextEngineClient() = default;
    
    bool Connect() {
        std::cout << "Mock client connected" << std::endl;
        return true;
    }
    
    void Disconnect() {
        std::cout << "Mock client disconnected" << std::endl;
    }
    
    bool IsConnected() const {
        return true;
    }
};

} // namespace nscfstore
