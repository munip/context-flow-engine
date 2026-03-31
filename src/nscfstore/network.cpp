#include "nscfstore/network.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

namespace nscfstore {

// Connection Implementation
Connection::Connection(int fd, ShardMemoryPool* pool) 
    : fd_(fd), memory_pool_(pool), active_(true), read_offset_(0), 
      header_received_(false), write_offset_(0) {
    
    read_buffer_.resize(64 * 1024); // 64KB read buffer
    write_buffer_.resize(64 * 1024); // 64KB write buffer
}

Connection::~Connection() {
    close();
}

bool Connection::read_request() {
    if (!active_) return false;
    
    // Try to read more data
    ssize_t bytes_read = ::read(fd_, read_buffer_.data() + read_offset_, 
                                read_buffer_.size() - read_offset_);
    
    if (bytes_read < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return false; // No data available now
        }
        std::cerr << "Read error: " << strerror(errno) << std::endl;
        close();
        return false;
    } else if (bytes_read == 0) {
        // Connection closed by client
        close();
        return false;
    }
    
    read_offset_ += bytes_read;
    
    // Try to parse header if not received yet
    if (!header_received_) {
        if (read_offset_ >= sizeof(MessageHeader)) {
            if (!parse_header()) {
                close();
                return false;
            }
            header_received_ = true;
        }
    }
    
    return true;
}

bool Connection::write_response(const Response& response) {
    if (!active_) return false;
    
    // Serialize response
    if (!serialize_response(response)) {
        return false;
    }
    
    // Write response
    ssize_t bytes_written = ::write(fd_, write_buffer_.data(), write_offset_);
    
    if (bytes_written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return false; // Would block
        }
        std::cerr << "Write error: " << strerror(errno) << std::endl;
        close();
        return false;
    }
    
    if (bytes_written < static_cast<ssize_t>(write_offset_)) {
        // Partial write, move remaining data
        memmove(write_buffer_.data(), write_buffer_.data() + bytes_written,
                write_offset_ - bytes_written);
        write_offset_ -= bytes_written;
    } else {
        write_offset_ = 0;
    }
    
    return true;
}

void Connection::close() {
    if (active_) {
        active_ = false;
        ::close(fd_);
    }
}

bool Connection::parse_header() {
    if (read_offset_ < sizeof(MessageHeader)) {
        return false;
    }
    
    memcpy(&current_header_, read_buffer_.data(), sizeof(MessageHeader));
    
    // Validate magic number
    if (current_header_.magic != 0xBEEF) {
        std::cerr << "Invalid magic number in header" << std::endl;
        return false;
    }
    
    // Check if we have the complete message
    if (read_offset_ < sizeof(MessageHeader) + current_header_.length) {
        return false;
    }
    
    return true;
}

bool Connection::parse_request(Request& request) {
    if (!header_received_) {
        return false;
    }
    
    // Parse request from buffer (simplified)
    request.type = static_cast<OperationType>(current_header_.op_type);
    request.request_id = current_header_.request_id;
    
    // Extract key and value from buffer
    char* data = read_buffer_.data() + sizeof(MessageHeader);
    size_t remaining = current_header_.length;
    
    // Simple parsing - in real implementation would use proper serialization
    if (remaining > 0) {
        request.key = std::string(data, std::min(remaining, size_t(256)));
        if (remaining > request.key.size() + 1) {
            request.value = std::string(data + request.key.size() + 1, 
                                     std::min(remaining - request.key.size() - 1, size_t(1024)));
        }
    }
    
    // Move remaining data to beginning of buffer
    size_t consumed = sizeof(MessageHeader) + current_header_.length;
    if (read_offset_ > consumed) {
        memmove(read_buffer_.data(), read_buffer_.data() + consumed,
                read_offset_ - consumed);
    }
    read_offset_ -= consumed;
    header_received_ = false;
    
    return true;
}

bool Connection::serialize_response(const Response& response) {
    write_offset_ = 0;
    
    // Serialize header
    MessageHeader header;
    header.magic = 0xBEEF; // Use 16-bit magic number
    header.version = 1;
    header.op_type = static_cast<uint8_t>(0); // Response doesn't have op_type
    header.length = 0; // Will be updated
    header.request_id = response.request_id;
    
    memcpy(write_buffer_.data(), &header, sizeof(MessageHeader));
    write_offset_ = sizeof(MessageHeader);
    
    // Serialize response data (simplified)
    std::string response_data = response.success ? "OK" : "ERROR";
    if (!response.value.empty()) {
        response_data = response.value;
    }
    
    if (write_offset_ + response_data.size() < write_buffer_.size()) {
        memcpy(write_buffer_.data() + write_offset_, response_data.data(), 
               response_data.size());
        write_offset_ += response_data.size();
    }
    
    // Update length in header
    header.length = response_data.size();
    memcpy(write_buffer_.data(), &header, sizeof(MessageHeader));
    
    return true;
}

// IoUringNetworkStack Implementation
IoUringNetworkStack::IoUringNetworkStack(uint32_t queue_depth)
    : queue_depth_(queue_depth), listen_fd_(-1), running_(false) {
    
    // Initialize io_uring
    int ret = io_uring_queue_init(queue_depth, &ring_, 0);
    if (ret < 0) {
        std::cerr << "Failed to initialize io_uring: " << -ret << std::endl;
        throw std::runtime_error("Failed to initialize io_uring");
    }
}

IoUringNetworkStack::~IoUringNetworkStack() {
    stop();
    io_uring_queue_exit(&ring_);
}

bool IoUringNetworkStack::start(uint16_t port) {
    if (running_) return true;
    
    // Create listening socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return false;
    }
    
    // Set socket to non-blocking
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);
    
    // Bind and listen
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    if (bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Failed to bind socket" << std::endl;
        ::close(listen_fd_);
        return false;
    }
    
    if (listen(listen_fd_, SOMAXCONN) < 0) {
        std::cerr << "Failed to listen on socket" << std::endl;
        ::close(listen_fd_);
        return false;
    }
    
    running_ = true;
    
    // Submit initial accept request
    submit_accept();
    
    std::cout << "IoUringNetworkStack started on port " << port << std::endl;
    return true;
}

void IoUringNetworkStack::stop() {
    if (!running_) return;
    
    running_ = false;
    
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    
    // Close all connections
    connections_.clear();
}

void IoUringNetworkStack::event_loop() {
    while (running_) {
        // Wait for completion events
        io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret < 0) {
            if (ret == -EINTR) continue;
            std::cerr << "io_uring_wait_cqe failed: " << -ret << std::endl;
            break;
        }
        
        process_completion(cqe);
        io_uring_cqe_seen(&ring_, cqe);
    }
}

void IoUringNetworkStack::submit_accept() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        std::cerr << "Failed to get submission queue entry for accept" << std::endl;
        return;
    }
    
    io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, nullptr); // Use nullptr as accept marker
    io_uring_submit(&ring_);
}

void IoUringNetworkStack::submit_read(Connection* conn) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        std::cerr << "Failed to get submission queue entry for read" << std::endl;
        return;
    }
    
    io_uring_prep_read(sqe, conn->fd(), conn->read_buffer().data() + conn->read_offset(),
                       conn->read_buffer().size() - conn->read_offset(), 0);
    io_uring_sqe_set_data(sqe, conn);
    io_uring_submit(&ring_);
}

void IoUringNetworkStack::submit_write(Connection* conn, const std::vector<char>& data) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        std::cerr << "Failed to get submission queue entry for write" << std::endl;
        return;
    }
    
    io_uring_prep_write(sqe, conn->fd(), data.data(), data.size(), 0);
    io_uring_sqe_set_data(sqe, conn);
    io_uring_submit(&ring_);
}

void IoUringNetworkStack::process_completion(io_uring_cqe* cqe) {
    void* user_data = io_uring_cqe_get_data(cqe);
    int res = cqe->res;
    
    if (user_data == nullptr) {
        // Accept completion
        if (res >= 0) {
            // New connection accepted
            auto conn = std::make_unique<Connection>(res, nullptr);
            connections_[res] = std::move(conn);
            
            // Submit read request for new connection
            submit_read(connections_[res].get());
            
            // Submit another accept
            submit_accept();
        } else {
            std::cerr << "Accept failed: " << -res << std::endl;
        }
    } else {
        // Read/Write completion
        Connection* conn = static_cast<Connection*>(user_data);
        
        if (res < 0) {
            // Error occurred, close connection
            connections_.erase(conn->fd());
        } else if (res == 0) {
            // Connection closed
            connections_.erase(conn->fd());
        } else {
            // Successful operation
            if (conn->is_active()) {
                // Submit another read request
                submit_read(conn);
            }
        }
    }
}

// NetworkServer Implementation
NetworkServer::NetworkServer(ShardManager* shard_manager, uint16_t port)
    : shard_manager_(shard_manager), port_(port), running_(false) {
    network_stack_ = std::make_unique<IoUringNetworkStack>();
}

NetworkServer::~NetworkServer() {
    stop();
}

bool NetworkServer::start() {
    if (running_) return true;
    
    if (!network_stack_->start(port_)) {
        return false;
    }
    
    // Start network worker thread
    network_thread_ = std::thread(&IoUringNetworkStack::event_loop, 
                                  network_stack_.get());
    
    running_ = true;
    return true;
}

void NetworkServer::stop() {
    if (!running_) return;
    
    running_ = false;
    network_stack_->stop();
    
    if (network_thread_.joinable()) {
        network_thread_.join();
    }
}

void NetworkServer::wait() {
    if (network_thread_.joinable()) {
        network_thread_.join();
    }
}

void NetworkServer::network_worker() {
    // This would handle the actual network processing
    // For now, the event_loop in IoUringNetworkStack handles this
}

} // namespace nscfstore
