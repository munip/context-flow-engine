#pragma once

#include <iostream>
#include <string>

namespace nscfstore {

// Simplified gRPC server for testing
class ContextEngineServiceImpl {
public:
    ContextEngineServiceImpl() = default;
    ~ContextEngineServiceImpl() = default;
    
    bool Start(const std::string& server_address) {
        std::cout << "Mock gRPC server started on " << server_address << std::endl;
        return true;
    }
    
    void Stop() {
        std::cout << "Mock gRPC server stopped" << std::endl;
    }
};

} // namespace nscfstore
