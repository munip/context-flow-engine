#pragma once

#include "proto/context_engine.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <memory>
#include <atomic>
#include <thread>

namespace nscfstore {

// Simplified gRPC server implementation
class ContextEngineServiceImpl {
public:
    ContextEngineServiceImpl() = default;
    ~ContextEngineServiceImpl() = default;
    
    bool Start(const std::string& server_address) {
        try {
            std::cout << "Starting gRPC server on " << server_address << std::endl;
            
            grpc::ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            
            // Add a completion queue to satisfy gRPC requirements
            completion_queue_ = builder.AddCompletionQueue();
            
            // Build server
            server_ = builder.BuildAndStart();
            
            if (!server_) {
                std::cerr << "Failed to build gRPC server" << std::endl;
                return false;
            }
            
            running_.store(true);
            std::cout << "✓ gRPC server started successfully" << std::endl;
            
            // Start server in background thread
            server_thread_ = std::thread([this]() {
                server_->Wait();
            });
            
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Failed to start gRPC server: " << e.what() << std::endl;
            return false;
        }
    }
    
    void Stop() {
        if (running_.load()) {
            running_.store(false);
            
            if (server_) {
                std::cout << "Stopping gRPC server..." << std::endl;
                server_->Shutdown();
                
                if (server_thread_.joinable()) {
                    server_thread_.join();
                }
                
                std::cout << "✓ gRPC server stopped" << std::endl;
            }
        }
    }
    
    bool IsRunning() const {
        return running_.load();
    }

private:
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<grpc::ServerCompletionQueue> completion_queue_;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
};

} // namespace nscfstore
