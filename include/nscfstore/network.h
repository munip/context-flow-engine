#pragma once

#include "common.h"
#include "memory_pool.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <liburing.h>
#include <vector>
#include <unordered_map>
#include <memory>
#include <atomic>

namespace nscfstore {

// Forward declarations
class ShardManager;

struct MessageHeader {
    uint16_t magic;
    uint8_t version;
    uint8_t op_type;
    uint32_t length;
    uint64_t request_id;
};

class Connection {
public:
    explicit Connection(int fd, ShardMemoryPool* pool);
    ~Connection();
    
    bool read_request();
    bool write_response(const Response& response);
    
    int fd() const { return fd_; }
    bool is_active() const { return active_.load(); }
    void close();
    
    // Accessors for io_uring operations
    std::vector<char>& read_buffer() { return read_buffer_; }
    std::vector<char>& write_buffer() { return write_buffer_; }
    size_t read_offset() const { return read_offset_; }
    size_t write_offset() const { return write_offset_; }
    
private:
    int fd_;
    ShardMemoryPool* memory_pool_;
    std::atomic<bool> active_;
    
    // Read state
    std::vector<char> read_buffer_;
    size_t read_offset_;
    bool header_received_;
    MessageHeader current_header_;
    
    // Write state
    std::vector<char> write_buffer_;
    size_t write_offset_;
    
    bool parse_header();
    bool parse_request(Request& request);
    bool serialize_response(const Response& response);
};

class IoUringNetworkStack {
public:
    explicit IoUringNetworkStack(uint32_t queue_depth = 4096);
    ~IoUringNetworkStack();
    
    bool start(uint16_t port);
    void stop();
    void event_loop();
    
    void submit_accept();
    void submit_read(Connection* conn);
    void submit_write(Connection* conn, const std::vector<char>& data);
    
    void process_completion(io_uring_cqe* cqe);
    
private:
    io_uring ring_;
    uint32_t queue_depth_;
    int listen_fd_;
    std::atomic<bool> running_;
    
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;
    std::mutex connections_mutex_;
    
    // Statistics
    std::atomic<uint64_t> connections_accepted_{0};
    std::atomic<uint64_t> bytes_received_{0};
    std::atomic<uint64_t> bytes_sent_{0};
    
    bool setup_listen_socket(uint16_t port);
    Connection* get_connection(int fd);
    void remove_connection(int fd);
    
    // Request routing
    void route_request(const Request& request, Connection* conn);
};

class NetworkServer {
public:
    explicit NetworkServer(ShardManager* shard_manager, uint16_t port = 8080);
    ~NetworkServer();
    
    bool start();
    void stop();
    void wait();
    
private:
    ShardManager* shard_manager_;
    uint16_t port_;
    std::unique_ptr<IoUringNetworkStack> network_stack_;
    std::thread network_thread_;
    std::atomic<bool> running_;
    
    void network_worker();
};

} // namespace nscfstore
