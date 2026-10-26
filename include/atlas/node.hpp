#pragma once

#include "atlas/order_book.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <filesystem>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>

namespace atlas {

struct NodeRaftStatus {
    std::uint64_t term{};
    std::uint8_t role{}; // 0 follower, 1 candidate, 2 leader
    std::uint64_t commit_index{};
    std::uint64_t last_applied{};
};

struct NodeConfig {
    std::string bind_address{"127.0.0.1"};
    std::uint16_t port{9000};
    std::size_t max_connections{64};
    std::size_t command_queue_capacity{256};
    std::size_t dedup_capacity{100000};
    std::size_t max_order_records{100000};
    std::uint32_t execution_delay_ms{0}; // Primarily useful for deterministic overload testing.
    std::uint64_t node_id{1};
    std::filesystem::path storage_directory;
    struct PeerEndpoint {
        std::uint64_t node_id{};
        std::string address;
        std::uint16_t port{};
    };
    std::vector<PeerEndpoint> peers;
    std::uint32_t heartbeat_interval_ms{50};
    std::uint32_t election_timeout_min_ms{250};
    std::uint32_t election_timeout_max_ms{500};
    std::size_t max_pending_requests{1024};
    bool replicated_mode{false};
};

class TcpNode {
public:
    explicit TcpNode(NodeConfig config = {});
    ~TcpNode();
    TcpNode(const TcpNode&) = delete;
    TcpNode& operator=(const TcpNode&) = delete;

    void start();
    void stop() noexcept;
    [[nodiscard]] std::uint16_t port() const noexcept;
    [[nodiscard]] std::uint64_t node_id() const noexcept;
    [[nodiscard]] bool replicated_mode() const noexcept;
    [[nodiscard]] NodeRaftStatus raft_status() const noexcept;
    // Returns a node-local snapshot. It is not a linearizable/leader read.
    [[nodiscard]] OrderBookState local_state(std::chrono::milliseconds timeout = std::chrono::seconds(5));

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace atlas
