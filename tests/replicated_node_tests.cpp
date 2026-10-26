#include "atlas/node.hpp"
#include "atlas/protocol.hpp"
#include "atlas/replicated_exchange.hpp"
#include "atlas/raft_storage.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <csignal>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <variant>
#include <vector>

namespace {
using atlas::NodeConfig;
using atlas::OrderBookState;
using atlas::OrderState;
using atlas::ReplicatedExchangeStateMachine;
using atlas::TcpNode;
namespace raft = atlas::raft;
using namespace atlas::protocol;

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

std::uint16_t reserve_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) throw std::runtime_error("cannot create port-reservation socket");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        ::close(fd);
        throw std::runtime_error("cannot reserve test port");
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        ::close(fd);
        throw std::runtime_error("cannot read test port");
    }
    const auto port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

std::filesystem::path unique_directory() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
        ("atlas_replicated_nodes_" + std::to_string(::getpid()) + "_" + std::to_string(now));
}

void send_message(int fd, const Message& message) {
    write_message(message, [fd](const std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
        for (;;) {
            const auto count = ::send(fd, data, size, MSG_NOSIGNAL);
            if (count < 0 && errno == EINTR) continue;
            return static_cast<std::ptrdiff_t>(count);
        }
    });
}

Message receive_message(int fd) {
    return read_message([fd](std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
        for (;;) {
            const auto count = ::recv(fd, data, size, 0);
            if (count < 0 && errno == EINTR) continue;
            return static_cast<std::ptrdiff_t>(count);
        }
    });
}

int connect_to(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) throw std::runtime_error("cannot create client socket");
    timeval timeout{};
    timeout.tv_sec = 10;
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        ::close(fd);
        throw std::runtime_error("cannot connect to elected leader");
    }
    return fd;
}

OrderResponse receive_order_response(int fd, std::uint64_t request_id,
                                     std::vector<TradeEvent>* trades = nullptr) {
    for (;;) {
        const auto message = receive_message(fd);
        if (const auto* response = std::get_if<OrderResponse>(&message)) {
            if (response->request_id == request_id) return *response;
        } else if (const auto* trade = std::get_if<TradeEvent>(&message)) {
            if (trades) trades->push_back(*trade);
        } else {
            throw std::runtime_error("unexpected response during replicated command");
        }
    }
}

bool same_order(const OrderState& lhs, const OrderState& rhs) {
    const auto& a = lhs.request;
    const auto& b = rhs.request;
    return a.id == b.id && a.client_id == b.client_id && a.request_id == b.request_id &&
        a.instrument_id == b.instrument_id && a.side == b.side && a.type == b.type &&
        a.limit_price == b.limit_price && a.quantity == b.quantity &&
        lhs.command_sequence == rhs.command_sequence && lhs.filled_quantity == rhs.filled_quantity &&
        lhs.leaves_quantity == rhs.leaves_quantity && lhs.canceled_quantity == rhs.canceled_quantity &&
        lhs.status == rhs.status;
}

void require_same_state(const OrderBookState& expected, const OrderBookState& actual) {
    require(expected.instrument_id == actual.instrument_id, "replicas agree on instrument");
    require(expected.next_command_sequence == actual.next_command_sequence,
            "replicas agree on command sequence");
    require(expected.next_trade_sequence == actual.next_trade_sequence,
            "replicas agree on trade sequence");
    require(expected.orders.size() == actual.orders.size(), "replicas agree on order count");
    for (std::size_t i = 0; i < expected.orders.size(); ++i)
        require(same_order(expected.orders[i], actual.orders[i]), "replicas agree on every order record");
}

std::array<std::uint16_t, 3> ports() {
    std::array<std::uint16_t, 3> result{};
    for (std::size_t i = 0; i < result.size(); ++i) {
        do { result[i] = reserve_port(); }
        while (std::find(result.begin(), result.begin() + static_cast<std::ptrdiff_t>(i), result[i]) !=
               result.begin() + static_cast<std::ptrdiff_t>(i));
    }
    return result;
}

std::array<NodeConfig, 3> configs(const std::array<std::uint16_t, 3>& node_ports,
                                 const std::filesystem::path& root) {
    std::array<NodeConfig, 3> result;
    for (std::size_t i = 0; i < result.size(); ++i) {
        result[i].bind_address = "127.0.0.1";
        result[i].port = node_ports[i];
        result[i].node_id = i + 1;
        result[i].replicated_mode = true;
        result[i].storage_directory = root / ("node-" + std::to_string(i + 1));
        result[i].heartbeat_interval_ms = 35;
        result[i].election_timeout_min_ms = 180;
        result[i].election_timeout_max_ms = 320;
        for (std::size_t peer = 0; peer < result.size(); ++peer) {
            if (peer != i) result[i].peers.push_back({peer + 1, "127.0.0.1", node_ports[peer]});
        }
    }
    return result;
}

bool wait_until(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

void await_cluster_applied(const std::vector<std::unique_ptr<TcpNode>>& nodes,
                           std::uint64_t index) {
    require(wait_until([&] {
        for (const auto& node : nodes) {
            const auto status = node->raft_status();
            if (status.commit_index < index || status.last_applied < index) return false;
        }
        return true;
    }, std::chrono::seconds(8)), "all nodes commit and apply each command");
}

std::size_t current_leader(const std::vector<std::unique_ptr<TcpNode>>& nodes) {
    std::size_t leader = nodes.size();
    require(wait_until([&] {
        leader = nodes.size();
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            if (nodes[i]->raft_status().role == 2) {
                if (leader != nodes.size()) return false;
                leader = i;
            }
        }
        return leader != nodes.size();
    }, std::chrono::seconds(8)), "three-node cluster elects a leader");
    return leader;
}

std::vector<std::unique_ptr<TcpNode>> start_cluster(const std::array<NodeConfig, 3>& settings) {
    std::vector<std::unique_ptr<TcpNode>> nodes;
    for (const auto& config : settings) {
        auto node = std::make_unique<TcpNode>(config);
        node->start();
        nodes.push_back(std::move(node));
    }
    return nodes;
}

void stop_cluster(std::vector<std::unique_ptr<TcpNode>>& nodes) {
    for (auto& node : nodes) if (node) node->stop();
    nodes.clear();
}

void test_three_node_replicated_exchange_and_recovery() {
    const auto root = unique_directory();
    const auto node_ports = ports();
    const auto settings = configs(node_ports, root);
    std::vector<std::unique_ptr<TcpNode>> nodes;
    int client = -1;
    try {
        nodes = start_cluster(settings);
        const std::size_t leader = current_leader(nodes);
        const std::size_t follower = (leader + 1) % nodes.size();
        const int follower_client = connect_to(node_ports[follower]);
        send_message(follower_client,
            SubmitOrder{99, 1, 9901, 1, Side::Buy, OrderType::Limit, 1, 1});
        const auto follower_result = receive_order_response(follower_client, 1);
        require(follower_result.result == ResultCode::NotLeader,
            "non-leader refuses writes with a retryable NotLeader response");
        ::shutdown(follower_client, SHUT_RDWR);
        ::close(follower_client);

        client = connect_to(node_ports[leader]);

        const SubmitOrder resting{101, 7, 1001, 1, Side::Sell, OrderType::Limit, 105, 5};
        send_message(client, resting);
        const auto first = receive_order_response(client, resting.request_id);
        require(first.result == ResultCode::Accepted && first.command_sequence == 1 &&
                first.leaves_quantity == 5, "first response follows committed and applied submit");
        await_cluster_applied(nodes, 1);

        const SubmitOrder incoming{202, 8, 2002, 1, Side::Buy, OrderType::Limit, 105, 2};
        send_message(client, incoming);
        std::vector<TradeEvent> trades;
        const auto second = receive_order_response(client, incoming.request_id, &trades);
        require(second.result == ResultCode::Accepted && second.status == OrderStatus::Filled &&
                trades.size() == 1 && trades.front().maker_order_id == resting.order_id &&
                trades.front().quantity == 2 && trades.front().price == 105,
                "replicated matching returns deterministic trade and terminal order response");
        await_cluster_applied(nodes, 2);

        const CancelOrder cancel{101, 9, resting.order_id};
        send_message(client, cancel);
        const auto canceled = receive_order_response(client, cancel.request_id);
        require(canceled.result == ResultCode::Accepted && canceled.status == OrderStatus::Canceled &&
                canceled.canceled_quantity == 3,
                "committed cancellation is applied (result=" +
                    std::to_string(static_cast<unsigned>(canceled.result)) + ", status=" +
                    std::to_string(static_cast<unsigned>(canceled.status)) + ", canceled=" +
                    std::to_string(canceled.canceled_quantity) + ")");
        await_cluster_applied(nodes, 3);
        ::shutdown(client, SHUT_RDWR);
        ::close(client);
        client = -1;

        const auto expected = nodes.front()->local_state();
        for (std::size_t i = 1; i < nodes.size(); ++i)
            require_same_state(expected, nodes[i]->local_state());
        stop_cluster(nodes);

        // Durable committed state and request IDs are reconstructed from each node's own log/store.
        nodes = start_cluster(settings);
        (void)current_leader(nodes);
        await_cluster_applied(nodes, 3);
        const auto recovered = nodes.front()->local_state();
        require_same_state(expected, recovered);
        for (std::size_t i = 1; i < nodes.size(); ++i)
            require_same_state(recovered, nodes[i]->local_state());

        const std::size_t retry_leader = current_leader(nodes);
        client = connect_to(node_ports[retry_leader]);
        send_message(client, incoming);
        std::vector<TradeEvent> retry_trades;
        const auto retry = receive_order_response(client, incoming.request_id, &retry_trades);
        require(retry.result == ResultCode::Accepted && retry.command_sequence == second.command_sequence &&
                retry_trades.empty(), "recovered exact retry returns original response without replaying trade events");
        auto conflicting_retry = incoming;
        conflicting_retry.quantity = 3;
        send_message(client, conflicting_retry);
        const auto conflict = receive_order_response(client, incoming.request_id);
        require(conflict.result == ResultCode::Duplicate,
            "reusing a recovered request ID with different bytes is rejected");
        ::shutdown(client, SHUT_RDWR);
        ::close(client);
        client = -1;
        stop_cluster(nodes);
    } catch (...) {
        if (client >= 0) { ::shutdown(client, SHUT_RDWR); ::close(client); }
        stop_cluster(nodes);
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
        throw;
    }
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

pid_t launch_node_process(const std::filesystem::path& executable, const NodeConfig& config) {
    std::vector<std::string> arguments{
        executable.string(), "--replicated", "--node-id", std::to_string(config.node_id),
        "--bind", config.bind_address, "--port", std::to_string(config.port),
        "--storage-dir", config.storage_directory.string(), "--heartbeat-ms",
        std::to_string(config.heartbeat_interval_ms), "--election-min-ms",
        std::to_string(config.election_timeout_min_ms), "--election-max-ms",
        std::to_string(config.election_timeout_max_ms)};
    for (const auto& peer : config.peers) {
        arguments.push_back("--peer");
        arguments.push_back(std::to_string(peer.node_id) + "," + peer.address + "," +
                            std::to_string(peer.port));
    }
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 1);
    for (auto& argument : arguments) argv.push_back(argument.data());
    argv.push_back(nullptr);
    const pid_t child = ::fork();
    if (child < 0) throw std::runtime_error("fork atlas-node process failed");
    if (child == 0) {
        ::execv(executable.c_str(), argv.data());
        _exit(127);
    }
    return child;
}

OrderResponse submit_to_any_leader(const std::array<std::uint16_t, 3>& node_ports,
                                   const SubmitOrder& request,
                                   std::size_t* accepted_node = nullptr) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(12);
    while (std::chrono::steady_clock::now() < deadline) {
        for (const auto port : node_ports) {
            int fd = -1;
            OrderResponse response;
            try {
                fd = connect_to(port);
                send_message(fd, request);
                response = receive_order_response(fd, request.request_id);
            } catch (const std::exception&) {
                if (fd >= 0) { ::shutdown(fd, SHUT_RDWR); ::close(fd); }
                continue;
            }
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
            if (response.result == ResultCode::NotLeader) continue;
            if (response.result != ResultCode::Accepted)
                throw std::runtime_error("process integration write was rejected");
            if (accepted_node) *accepted_node = static_cast<std::size_t>(
                std::find(node_ports.begin(), node_ports.end(), port) - node_ports.begin());
            return response;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
    }
    throw std::runtime_error("no process accepted a committed exchange write");
}

void stop_node_processes(std::array<pid_t, 3>& children) {
    for (const auto child : children) if (child > 0) (void)::kill(child, SIGTERM);
    for (auto& child : children) {
        if (child <= 0) continue;
        int status = 0;
        if (::waitpid(child, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
            throw std::runtime_error("atlas-node child did not stop cleanly");
        child = -1;
    }
}

void test_three_independent_node_processes() {
    const auto root = unique_directory();
    const auto node_ports = ports();
    const auto settings = configs(node_ports, root);
    const auto executable = std::filesystem::canonical("/proc/self/exe").parent_path() / "atlas-node";
    require(std::filesystem::exists(executable), "atlas-node executable is available beside the integration test");
    std::array<pid_t, 3> children{-1, -1, -1};
    try {
        for (std::size_t i = 0; i < children.size(); ++i)
            children[i] = launch_node_process(executable, settings[i]);

        const auto first = submit_to_any_leader(node_ports,
            SubmitOrder{301, 1, 3001, 1, Side::Sell, OrderType::Limit, 110, 4});
        const auto second = submit_to_any_leader(node_ports,
            SubmitOrder{302, 1, 3002, 1, Side::Buy, OrderType::Limit, 90, 3});
        std::size_t leader_index = 0;
        const auto third = submit_to_any_leader(node_ports,
            SubmitOrder{303, 1, 3003, 1, Side::Buy, OrderType::Limit, 95, 2}, &leader_index);
        require(first.result == ResultCode::Accepted && second.result == ResultCode::Accepted &&
                third.result == ResultCode::Accepted, "separate processes commit the ordered command stream");

        require(wait_until([&] {
            for (const auto& config : settings) {
                try {
                    raft::FilePersistentStorage storage(config.storage_directory);
                    if (storage.load_commit_index() < 3) return false;
                } catch (...) { return false; }
            }
            return true;
        }, std::chrono::seconds(8)), "all independent process stores persist the committed prefix");

        require(::kill(children[leader_index], SIGTERM) == 0, "stop the current leader process");
        int stopped_status = 0;
        require(::waitpid(children[leader_index], &stopped_status, 0) == children[leader_index] &&
                WIFEXITED(stopped_status) && WEXITSTATUS(stopped_status) == 0,
                "current leader process exits cleanly");
        children[leader_index] = -1;
        const auto after_change = submit_to_any_leader(node_ports,
            SubmitOrder{304, 1, 3004, 1, Side::Buy, OrderType::Limit, 85, 1});
        require(after_change.result == ResultCode::Accepted,
                "remaining majority elects a leader and commits after leader loss");
        children[leader_index] = launch_node_process(executable, settings[leader_index]);

        require(wait_until([&] {
            for (const auto& config : settings) {
                try {
                    raft::FilePersistentStorage storage(config.storage_directory);
                    if (storage.load_commit_index() < 4) return false;
                } catch (...) { return false; }
            }
            return true;
        }, std::chrono::seconds(8)), "restarted process catches up through the new committed entry");

        std::size_t retry_node = 0;
        const SubmitOrder disconnected{305, 1, 3005, 1, Side::Sell, OrderType::Limit, 120, 2};
        (void)submit_to_any_leader(node_ports, disconnected, &retry_node);
        int disconnected_fd = connect_to(node_ports[retry_node]);
        const SubmitOrder uncertain{306, 1, 3006, 1, Side::Sell, OrderType::Limit, 125, 3};
        send_message(disconnected_fd, uncertain);
        (void)::shutdown(disconnected_fd, SHUT_WR);
        ::close(disconnected_fd); // The client intentionally does not read its commit response.
        require(wait_until([&] {
            for (const auto& config : settings) {
                try {
                    raft::FilePersistentStorage storage(config.storage_directory);
                    if (storage.load_commit_index() < 6) return false;
                } catch (...) { return false; }
            }
            return true;
        }, std::chrono::seconds(8)), "disconnected client's command still reaches committed logs");
        const auto retry_response = submit_to_any_leader(node_ports, uncertain);
        require(retry_response.result == ResultCode::Accepted,
                "exact retry resolves a disconnected client's uncertain acknowledgement");
        stop_node_processes(children);

        std::array<std::unique_ptr<raft::FilePersistentStorage>, 3> stores;
        std::array<std::unique_ptr<ReplicatedExchangeStateMachine>, 3> machines;
        for (std::size_t i = 0; i < stores.size(); ++i) {
            stores[i] = std::make_unique<raft::FilePersistentStorage>(settings[i].storage_directory);
            machines[i] = std::make_unique<ReplicatedExchangeStateMachine>(*stores[i]);
            const auto log = stores[i]->load_log();
            const auto committed = stores[i]->load_commit_index();
            require(committed == 6 && log.size() >= committed, "each process retains all committed commands");
            machines[i]->restore_committed(std::vector<raft::LogEntry>(
                log.begin(), log.begin() + static_cast<std::ptrdiff_t>(committed)));
        }
        const auto expected = machines[0]->book().export_state();
        for (std::size_t i = 1; i < machines.size(); ++i)
            require_same_state(expected, machines[i]->book().export_state());
    } catch (...) {
        for (const auto child : children) if (child > 0) (void)::kill(child, SIGTERM);
        for (auto& child : children) if (child > 0) (void)::waitpid(child, nullptr, 0);
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
        throw;
    }
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

void test_no_success_without_a_quorum() {
    const auto root = unique_directory();
    const auto node_ports = ports();
    const auto settings = configs(node_ports, root);
    auto nodes = start_cluster(settings);
    int client = -1;
    try {
        const auto leader = current_leader(nodes);
        for (std::size_t i = 0; i < nodes.size(); ++i)
            if (i != leader) nodes[i]->stop();

        client = connect_to(node_ports[leader]);
        timeval timeout{};
        timeout.tv_sec = 0;
        timeout.tv_usec = 400000;
        (void)::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        const SubmitOrder isolated{401, 1, 4001, 1, Side::Buy, OrderType::Limit, 90, 1};
        send_message(client, isolated);
        pollfd descriptor{client, POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, 400);
        if (ready > 0 && (descriptor.revents & POLLIN) != 0) {
            const auto response = receive_order_response(client, isolated.request_id);
            require(response.result != ResultCode::Accepted,
                    "server never reports success for an entry without quorum commitment/application");
        } else {
            // No response while the old leader is isolated is expected temporary unavailability.
            const auto status = nodes[leader]->raft_status();
            require(status.commit_index == 0 && status.last_applied == 0,
                    "an uncommitted isolated command does not advance applied state");
        }
        ::shutdown(client, SHUT_RDWR);
        ::close(client);
        client = -1;
        stop_cluster(nodes);
    } catch (...) {
        if (client >= 0) { ::shutdown(client, SHUT_RDWR); ::close(client); }
        stop_cluster(nodes);
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
        throw;
    }
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

} // namespace

int main() {
    try {
        test_three_node_replicated_exchange_and_recovery();
        test_three_independent_node_processes();
        test_no_success_without_a_quorum();
        std::cout << "atlas_replicated_node_tests: three-node replication, process, and recovery tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas_replicated_node_tests: " << error.what() << '\n';
        return 1;
    }
}
