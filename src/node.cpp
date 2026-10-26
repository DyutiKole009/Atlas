#include "atlas/node.hpp"

#include "atlas/order_book.hpp"
#include "atlas/protocol.hpp"
#include "atlas/raft.hpp"
#include "atlas/raft_storage.hpp"
#include "atlas/raft_transport.hpp"
#include "atlas/replicated_exchange.hpp"

#include <arpa/inet.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <cstring>
#include <future>
#include <iostream>
#include <fcntl.h>
#include <poll.h>
#include <random>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <set>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace atlas {
namespace {

using protocol::Message;
using RequestKey = std::pair<std::uint64_t, std::uint64_t>;

class SystemRaftClock final : public raft::Clock {
public:
    raft::Milliseconds now_ms() const noexcept override {
        return static_cast<raft::Milliseconds>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    }
};

class RandomElectionTimeout final : public raft::ElectionTimeoutSource {
public:
    RandomElectionTimeout(std::uint32_t minimum, std::uint32_t maximum)
        : distribution_(minimum, maximum), generator_(std::random_device{}()) {}
    raft::Milliseconds next_timeout_ms() override { return distribution_(generator_); }
private:
    std::uniform_int_distribution<std::uint32_t> distribution_;
    std::mt19937 generator_;
};

class BoundedCommandQueue {
public:
    explicit BoundedCommandQueue(std::size_t capacity) : capacity_(capacity) {}

    bool try_push(std::shared_ptr<struct WorkItem> item) {
        std::lock_guard lock(mutex_);
        if (closed_ || items_.size() >= capacity_) return false;
        items_.push_back(std::move(item));
        ready_.notify_one();
        return true;
    }

    std::shared_ptr<struct WorkItem> pop_for(std::chrono::milliseconds duration) {
        std::unique_lock lock(mutex_);
        ready_.wait_for(lock, duration, [&] { return closed_ || !items_.empty(); });
        if (items_.empty()) return {};
        auto item = std::move(items_.front());
        items_.pop_front();
        return item;
    }

    void close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        ready_.notify_all();
    }

private:
    const std::size_t capacity_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::shared_ptr<struct WorkItem>> items_;
    bool closed_{false};
};

struct WorkItem {
    Message message;
    std::promise<std::vector<Message>> completion;
    std::optional<std::uint64_t> raft_sender;
    std::optional<raft::Message> raft_message;
    bool snapshot_query{false};
    std::promise<OrderBookState> state_completion;
};

struct PendingRequest {
    std::vector<std::uint8_t> fingerprint;
    struct Waiter {
        std::shared_ptr<WorkItem> item;
        bool retry{false};
    };
    std::vector<Waiter> waiters;
};

struct DedupEntry {
    std::vector<std::uint8_t> fingerprint;
    protocol::OrderResponse response;
};

protocol::ResultCode submit_result_code(SubmitError error) {
    switch (error) {
    case SubmitError::None: return protocol::ResultCode::Accepted;
    case SubmitError::DuplicateOrderId:
    case SubmitError::DuplicateRequestId: return protocol::ResultCode::Duplicate;
    default: return protocol::ResultCode::Invalid;
    }
}

protocol::OrderStatus protocol_status(OrderStatus status) {
    switch (status) {
    case OrderStatus::Open: return protocol::OrderStatus::Open;
    case OrderStatus::PartiallyFilled: return protocol::OrderStatus::PartiallyFilled;
    case OrderStatus::Filled: return protocol::OrderStatus::Filled;
    case OrderStatus::Canceled: return protocol::OrderStatus::Canceled;
    }
    return protocol::OrderStatus::Rejected;
}

protocol::OrderResponse rejection(std::uint64_t client_id, std::uint64_t request_id,
                                  std::uint64_t order_id, protocol::ResultCode code) {
    return protocol::OrderResponse{client_id, request_id, order_id, 0,
        protocol::OrderStatus::Rejected, code, 0, 0, 0};
}

} // namespace

class TcpNode::Impl {
public:
    explicit Impl(NodeConfig config)
        : config_(std::move(config)), queue_(config_.command_queue_capacity) {
        if (config_.max_connections == 0 || config_.command_queue_capacity == 0 ||
            config_.dedup_capacity == 0 || config_.max_order_records == 0) {
            throw std::invalid_argument("node resource limits must be nonzero");
        }
        if (config_.replicated_mode) {
            if (config_.node_id == 0 || config_.storage_directory.empty() || config_.peers.size() != 2 ||
                config_.election_timeout_min_ms == 0 ||
                config_.election_timeout_max_ms <= config_.election_timeout_min_ms ||
                config_.heartbeat_interval_ms == 0 || config_.max_pending_requests == 0) {
                throw std::invalid_argument("replicated mode requires node ID, storage, two peers, and valid timer/queue limits");
            }
            std::set<std::uint64_t> ids{config_.node_id};
            std::set<std::pair<std::string, std::uint16_t>> endpoints;
            if (config_.port != 0) endpoints.emplace(config_.bind_address, config_.port);
            for (const auto& peer : config_.peers) {
                in_addr peer_address{};
                if (peer.node_id == 0 || peer.port == 0 || peer.address.empty() ||
                    ::inet_pton(AF_INET, peer.address.c_str(), &peer_address) != 1 ||
                    !ids.insert(peer.node_id).second ||
                    !endpoints.emplace(peer.address, peer.port).second) {
                    throw std::invalid_argument("Raft peers require unique IDs/endpoints and valid IPv4 addresses");
                }
            }
            if (ids.size() != 3) throw std::invalid_argument("replicated exchange requires exactly three distinct node IDs");
        }
        in_addr address{};
        if (::inet_pton(AF_INET, config_.bind_address.c_str(), &address) != 1) {
            throw std::invalid_argument("bind address must be an IPv4 numeric address");
        }
    }

    ~Impl() { stop(); }

    void start() {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true)) {
            throw std::logic_error("node already started");
        }
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) throw std::runtime_error("socket: " + std::string(std::strerror(errno)));
        int reuse = 1;
        (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(config_.port);
        if (::inet_pton(AF_INET, config_.bind_address.c_str(), &address.sin_addr) != 1 ||
            ::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            ::listen(fd, static_cast<int>(config_.max_connections)) != 0) {
            const int error = errno;
            ::close(fd);
            started_ = false;
            throw std::runtime_error("bind/listen: " + std::string(std::strerror(error)));
        }
        socklen_t address_size = sizeof(address);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
            const int error = errno;
            ::close(fd);
            started_ = false;
            throw std::runtime_error("getsockname: " + std::string(std::strerror(error)));
        }
        bound_port_ = ntohs(address.sin_port);
        listen_fd_ = fd;
        stopped_ = false;
        if (config_.replicated_mode) {
            raft_storage_ = std::make_unique<raft::FilePersistentStorage>(config_.storage_directory);
            state_machine_ = std::make_unique<ReplicatedExchangeStateMachine>(*raft_storage_);
            raft_clock_ = std::make_unique<SystemRaftClock>();
            timeout_source_ = std::make_unique<RandomElectionTimeout>(
                config_.election_timeout_min_ms, config_.election_timeout_max_ms);
            std::vector<raft::NodeId> peers;
            for (const auto& peer : config_.peers) peers.push_back(peer.node_id);
            const auto persistent_log = raft_storage_->load_log();
            const raft::LogMetadata metadata{persistent_log.empty() ? 0 : persistent_log.back().term,
                                              static_cast<raft::LogIndex>(persistent_log.size())};
            raft_node_ = std::make_unique<raft::RaftNode>(config_.node_id, std::move(peers), metadata,
                *raft_storage_, *raft_clock_, *timeout_source_, state_machine_.get(),
                config_.heartbeat_interval_ms);
            if (raft_node_->last_applied() > 0) {
                for (raft::LogIndex index = 1; index <= raft_node_->last_applied(); ++index) {
                    const auto& entry = raft_node_->log()[index - 1];
                    const auto request = protocol::decode(entry.command);
                    const auto [client_id, request_id, order_id] = request_identity(request);
                    (void)order_id;
                    dedup_fingerprints_.try_emplace(RequestKey{client_id, request_id}, protocol::encode(request));
                }
            }
        }
        if (config_.replicated_mode) {
            for (const auto& peer : config_.peers)
                transport_threads_.emplace_back([this, id = peer.node_id] { transport_loop(id); });
        }
        execution_thread_ = std::thread([this] { execution_loop(); });
        accept_thread_ = std::thread([this] { accept_loop(); });
    }

    void stop() noexcept {
        if (!started_.load() || stopped_.exchange(true)) return;
        const int listener = listen_fd_.exchange(-1);
        if (listener >= 0) {
            ::shutdown(listener, SHUT_RDWR);
            ::close(listener);
        }
        {
            std::lock_guard lock(clients_mutex_);
            for (int fd : client_fds_) ::shutdown(fd, SHUT_RDWR);
        }
        queue_.close();
        {
            std::lock_guard lock(transport_mutex_);
            transport_stopping_ = true;
        }
        transport_ready_.notify_all();
        if (accept_thread_.joinable()) accept_thread_.join();
        if (execution_thread_.joinable()) execution_thread_.join();
        for (auto& thread : transport_threads_) if (thread.joinable()) thread.join();
    }

    std::uint16_t port() const noexcept { return bound_port_; }
    std::uint64_t node_id() const noexcept { return config_.node_id; }
    bool replicated_mode() const noexcept { return config_.replicated_mode; }
    NodeRaftStatus raft_status() const noexcept {
        return {published_term_.load(), published_role_.load(), published_commit_.load(), published_applied_.load()};
    }
    OrderBookState local_state(std::chrono::milliseconds timeout) {
        if (!config_.replicated_mode) throw std::logic_error("local replicated state is available only in cluster mode");
        if (replicated_failed_) throw std::runtime_error("replicated node is unavailable after an execution/storage failure");
        auto item = std::make_shared<WorkItem>();
        item->snapshot_query = true;
        auto future = item->state_completion.get_future();
        if (!queue_.try_push(item) || future.wait_for(timeout) != std::future_status::ready)
            throw std::runtime_error("timed out waiting for local execution-thread snapshot");
        return future.get();
    }

private:
    struct Worker {
        int fd{-1};
        std::atomic<bool> finished{false};
        std::thread thread;
    };

    void accept_loop() {
        std::vector<std::unique_ptr<Worker>> workers;
        while (!stopped_) {
            for (auto it = workers.begin(); it != workers.end();) {
                if ((*it)->finished.load()) {
                    if ((*it)->thread.joinable()) (*it)->thread.join();
                    it = workers.erase(it);
                } else {
                    ++it;
                }
            }
            const int listener = listen_fd_.load();
            if (listener < 0) break;
            sockaddr_in peer{};
            socklen_t peer_size = sizeof(peer);
            const int client_fd = ::accept4(listener, reinterpret_cast<sockaddr*>(&peer), &peer_size, SOCK_CLOEXEC);
            if (client_fd < 0) {
                if (errno == EINTR) continue;
                if (stopped_ || errno == EBADF || errno == EINVAL) break;
                continue;
            }
            if (stopped_) { ::close(client_fd); break; }
            if (workers.size() >= config_.max_connections) {
                ::shutdown(client_fd, SHUT_RDWR);
                ::close(client_fd);
                continue;
            }
            timeval timeout{};
            timeout.tv_sec = 5;
            (void)::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            (void)::setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            auto worker = std::make_unique<Worker>();
            worker->fd = client_fd;
            {
                std::lock_guard lock(clients_mutex_);
                client_fds_.insert(client_fd);
            }
            Worker* worker_ptr = worker.get();
            worker->thread = std::thread([this, worker_ptr] {
                handle_client(worker_ptr->fd);
                {
                    std::lock_guard lock(clients_mutex_);
                    client_fds_.erase(worker_ptr->fd);
                }
                ::shutdown(worker_ptr->fd, SHUT_RDWR);
                ::close(worker_ptr->fd);
                worker_ptr->finished = true;
            });
            workers.push_back(std::move(worker));
        }
        {
            std::lock_guard lock(clients_mutex_);
            for (int fd : client_fds_) ::shutdown(fd, SHUT_RDWR);
        }
        for (auto& worker : workers) if (worker->thread.joinable()) worker->thread.join();
    }

    void handle_client(int fd) noexcept {
        while (!stopped_) {
            Message message;
            try {
                message = protocol::read_message([fd](std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
                    for (;;) {
                        const ssize_t count = ::recv(fd, data, size, 0);
                        if (count < 0 && errno == EINTR) continue;
                        return static_cast<std::ptrdiff_t>(count);
                    }
                });
            } catch (...) {
                return; // malformed frame, EOF, timeout, or socket error closes only this client
            }

            if (const auto* envelope = std::get_if<protocol::RaftTransportEnvelope>(&message)) {
                if (!config_.replicated_mode || !peer_endpoint(envelope->sender_id).has_value()) return;
                try {
                    const protocol::Message nested = protocol::decode(envelope->rpc_frame);
                    auto item = std::make_shared<WorkItem>();
                    item->raft_message = raft::from_protocol_message(nested);
                    item->raft_sender = envelope->sender_id;
                    if (!queue_.try_push(item)) return;
                } catch (...) {
                    return;
                }
                return; // response is sent on a new outbound peer connection by the execution owner
            }

            if (const auto* heartbeat = std::get_if<protocol::Heartbeat>(&message)) {
                if (heartbeat->kind != protocol::HeartbeatKind::Ping) return;
                if (!send_message(fd, protocol::Heartbeat{heartbeat->client_id, heartbeat->request_id,
                                                         heartbeat->nonce, protocol::HeartbeatKind::Pong})) return;
                continue;
            }
            if (!std::holds_alternative<protocol::SubmitOrder>(message) &&
                !std::holds_alternative<protocol::CancelOrder>(message)) return;

            const std::uint64_t client_id = std::visit([](const auto& request) -> std::uint64_t {
                using T = std::decay_t<decltype(request)>;
                if constexpr (std::is_same_v<T, protocol::SubmitOrder> || std::is_same_v<T, protocol::CancelOrder>)
                    return request.client_id;
                return 0;
            }, message);
            const std::uint64_t request_id = std::visit([](const auto& request) -> std::uint64_t {
                using T = std::decay_t<decltype(request)>;
                if constexpr (std::is_same_v<T, protocol::SubmitOrder> || std::is_same_v<T, protocol::CancelOrder>)
                    return request.request_id;
                return 0;
            }, message);
            auto item = std::make_shared<WorkItem>();
            item->message = std::move(message);
            auto future = item->completion.get_future();
            if (!queue_.try_push(item)) {
                if (!send_message(fd, rejection(client_id, request_id,
                        std::holds_alternative<protocol::SubmitOrder>(item->message)
                            ? std::get<protocol::SubmitOrder>(item->message).order_id
                            : std::get<protocol::CancelOrder>(item->message).order_id,
                        protocol::ResultCode::InternalError))) return;
                continue;
            }
            try {
                if (future.wait_for(std::chrono::seconds(10)) != std::future_status::ready) return;
                const std::vector<Message> responses = future.get();
                for (const Message& response : responses) {
                    if (!send_message(fd, response)) return; // slow or disconnected clients are isolated
                }
            } catch (...) {
                return;
            }
        }
    }

    static bool send_message(int fd, const Message& message) noexcept {
        try {
            protocol::write_message(message, [fd](const std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
                for (;;) {
                    const ssize_t count = ::send(fd, data, size, MSG_NOSIGNAL);
                    if (count < 0 && errno == EINTR) continue;
                    return static_cast<std::ptrdiff_t>(count);
                }
            });
            return true;
        } catch (...) {
            return false;
        }
    }

    void execution_loop() noexcept {
        for (;;) {
            const auto item = queue_.pop_for(std::chrono::milliseconds(config_.replicated_mode ? 5 : 1000));
            if (!item) {
                if (queue_closed()) return;
                if (config_.replicated_mode) {
                    try { drive_raft_tick(); }
                    catch (const std::exception& error) {
                        std::cerr << "atlas-node Raft tick failed: " << error.what() << '\n';
                        replicated_failed_ = true;
                        fail_pending();
                    }
                }
                continue;
            }
            try {
                if (config_.execution_delay_ms != 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(config_.execution_delay_ms));
                }
                if (config_.replicated_mode) {
                    if (replicated_failed_) {
                        if (!item->raft_sender && !item->snapshot_query) {
                            item->completion.set_value({rejection(
                                std::get<0>(request_identity(item->message)),
                                std::get<1>(request_identity(item->message)),
                                std::get<2>(request_identity(item->message)),
                                protocol::ResultCode::InternalError)});
                        } else if (item->snapshot_query) {
                            throw std::runtime_error("replicated node stopped after an execution/storage failure");
                        }
                        continue;
                    }
                    if (item->raft_sender) {
                        const raft::Role before = raft_node_->role();
                        const auto applied = raft_node_->receive_applied(*item->raft_sender,
                            *item->raft_message);
                        finish_applied(applied);
                        if (before == raft::Role::Leader && raft_node_->role() != raft::Role::Leader) fail_pending();
                        flush_raft_outbox();
                    } else {
                        if (item->snapshot_query) item->state_completion.set_value(state_machine_->book().export_state());
                        else execute_replicated(item);
                    }
                    drive_raft_tick();
                    publish_raft_status();
                } else {
                    item->completion.set_value(execute(item->message));
                }
            } catch (...) {
                if (config_.replicated_mode) {
                    replicated_failed_ = true;
                    fail_pending();
                }
                try { item->completion.set_exception(std::current_exception()); } catch (...) {}
            }
        }
    }

    bool queue_closed() const noexcept { return stopped_.load(); }

    std::optional<NodeConfig::PeerEndpoint> peer_endpoint(std::uint64_t id) const {
        for (const auto& peer : config_.peers) if (peer.node_id == id) return peer;
        return std::nullopt;
    }

    void execute_replicated(const std::shared_ptr<WorkItem>& item) {
        if (replicated_failed_) {
            const auto [client_id, request_id, order_id] = request_identity(item->message);
            item->completion.set_value({rejection(client_id, request_id, order_id,
                                                   protocol::ResultCode::InternalError)});
            return;
        }
        if (raft_node_->role() != raft::Role::Leader) {
            const auto [client_id, request_id, order_id] = request_identity(item->message);
            item->completion.set_value({rejection(client_id, request_id, order_id,
                                                   protocol::ResultCode::NotLeader)});
            return;
        }
        const auto identity = request_identity(item->message);
        const RequestKey key{std::get<0>(identity), std::get<1>(identity)};
        const auto fingerprint = protocol::encode(item->message);
        if (const auto applied = state_machine_->result(key.first, key.second)) {
            const auto prior = dedup_fingerprints_.find(key);
            if (prior == dedup_fingerprints_.end() || prior->second != fingerprint) {
                item->completion.set_value({rejection(key.first, key.second, std::get<2>(identity),
                                                       protocol::ResultCode::Duplicate)});
            } else {
                item->completion.set_value(applied->messages);
            }
            return;
        }
        auto pending = pending_requests_.find(key);
        if (pending != pending_requests_.end()) {
            if (pending->second.fingerprint != fingerprint) {
                item->completion.set_value({rejection(key.first, key.second, std::get<2>(identity),
                                                       protocol::ResultCode::Duplicate)});
            } else {
                pending->second.waiters.push_back({item, true});
            }
            return;
        }
        if (pending_requests_.size() >= config_.max_pending_requests) {
            item->completion.set_value({rejection(key.first, key.second, std::get<2>(identity),
                                                   protocol::ResultCode::InternalError)});
            return;
        }
        const raft::LogIndex index = raft_node_->append_command(encode_replicated_command(item->message));
        PendingRequest request{fingerprint, {{item, false}}};
        pending_requests_.emplace(key, request);
        requests_by_index_[index] = key;
        dedup_fingerprints_[key] = fingerprint;
        flush_raft_outbox();
        const auto newly_applied = raft_node_->apply_committed();
        finish_applied(newly_applied);
    }

    static std::tuple<std::uint64_t, std::uint64_t, std::uint64_t> request_identity(const Message& message) {
        if (const auto* submit = std::get_if<protocol::SubmitOrder>(&message))
            return {submit->client_id, submit->request_id, submit->order_id};
        if (const auto* cancel = std::get_if<protocol::CancelOrder>(&message))
            return {cancel->client_id, cancel->request_id, cancel->order_id};
        throw protocol::ProtocolError("expected a replicated client command");
    }

    void drive_raft_tick() {
        if (!config_.replicated_mode || stopped_ || replicated_failed_) return;
        const raft::Role before = raft_node_->role();
        (void)raft_node_->poll();
        const auto applied = raft_node_->apply_committed();
        finish_applied(applied);
        if (before == raft::Role::Leader && raft_node_->role() != raft::Role::Leader) fail_pending();
        flush_raft_outbox();
        publish_raft_status();
    }

    void publish_raft_status() noexcept {
        if (!raft_node_) return;
        published_term_ = raft_node_->current_term();
        published_role_ = static_cast<std::uint8_t>(raft_node_->role());
        published_commit_ = raft_node_->commit_index();
        published_applied_ = raft_node_->last_applied();
    }

    void finish_applied(const std::vector<raft::LogIndex>& indexes) {
        for (const raft::LogIndex index : indexes) {
            if (index == 0 || index > raft_node_->log().size()) continue;
            const Message command = protocol::decode(raft_node_->log()[index - 1].command);
            const auto [client_id, request_id, order_id] = request_identity(command);
            const RequestKey key{client_id, request_id};
            const auto result = state_machine_->take_applied_result(index);
            if (!result) continue;
            const auto retry_result = state_machine_->result(client_id, request_id);
            const auto waiters = pending_requests_.find(key);
            if (waiters != pending_requests_.end()) {
                for (const auto& waiter : waiters->second.waiters) {
                    const auto& messages = waiter.retry && retry_result ? retry_result->messages : result->messages;
                    try { waiter.item->completion.set_value(messages); } catch (...) {}
                }
                pending_requests_.erase(waiters);
            }
            requests_by_index_.erase(index);
            dedup_fingerprints_.try_emplace(key, protocol::encode(command));
            (void)order_id;
        }
    }

    void fail_pending() noexcept {
        for (auto& [key, pending] : pending_requests_) {
            const auto order_id = std::get<2>(request_identity(protocol::decode(pending.fingerprint)));
            for (const auto& waiter : pending.waiters) {
                try { waiter.item->completion.set_value({rejection(key.first, key.second, order_id,
                    protocol::ResultCode::InternalError)}); } catch (...) {}
            }
            if (!state_machine_->result(key.first, key.second)) dedup_fingerprints_.erase(key);
        }
        pending_requests_.clear();
        requests_by_index_.clear();
    }

    void flush_raft_outbox() {
        for (const auto& outbound : raft_node_->drain_outbox()) {
            const auto endpoint = peer_endpoint(outbound.target);
            if (!endpoint) continue;
            const auto rpc_frame = protocol::encode(raft::to_protocol_message(outbound.message));
            const Message envelope = protocol::RaftTransportEnvelope{config_.node_id, rpc_frame};
            {
                std::lock_guard lock(transport_mutex_);
                auto& peer_queue = peer_outbound_[outbound.target];
                if (transport_stopping_ || peer_queue.size() >= 1024) continue;
                peer_queue.push_back(envelope);
            }
            transport_ready_.notify_all();
        }
    }

    void transport_loop(std::uint64_t peer_id) noexcept {
        for (;;) {
            Message message;
            {
                std::unique_lock lock(transport_mutex_);
                transport_ready_.wait(lock, [&] {
                    const auto it = peer_outbound_.find(peer_id);
                    return transport_stopping_ || (it != peer_outbound_.end() && !it->second.empty());
                });
                if (transport_stopping_) return;
                auto& peer_queue = peer_outbound_[peer_id];
                message = std::move(peer_queue.front());
                peer_queue.pop_front();
            }
            const auto endpoint = peer_endpoint(peer_id);
            if (endpoint) send_to_peer(*endpoint, message);
        }
    }

    static void send_to_peer(const NodeConfig::PeerEndpoint& peer, const Message& message) {
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) return;
        const int old_flags = ::fcntl(fd, F_GETFL, 0);
        (void)::fcntl(fd, F_SETFL, old_flags | O_NONBLOCK);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(peer.port);
        if (::inet_pton(AF_INET, peer.address.c_str(), &address.sin_addr) != 1) { ::close(fd); return; }
        int connected = ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
        if (connected != 0 && errno != EINPROGRESS) { ::close(fd); return; }
        if (connected != 0) {
            pollfd poll_descriptor{fd, POLLOUT, 0};
            if (::poll(&poll_descriptor, 1, 300) <= 0) { ::close(fd); return; }
            int error = 0; socklen_t error_size = sizeof(error);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0 || error != 0) { ::close(fd); return; }
        }
        timeval timeout{}; timeout.tv_sec = 0; timeout.tv_usec = 300000;
        (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        (void)::fcntl(fd, F_SETFL, old_flags & ~O_NONBLOCK);
        try {
            protocol::write_message(message, [fd](const std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
                for (;;) { const ssize_t n = ::send(fd, data, size, MSG_NOSIGNAL); if (n < 0 && errno == EINTR) continue; return n; }
            });
        } catch (...) {}
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }

    std::vector<Message> execute(const Message& message) {
        const RequestKey key = std::visit([](const auto& request) -> RequestKey {
            using T = std::decay_t<decltype(request)>;
            if constexpr (std::is_same_v<T, protocol::SubmitOrder> || std::is_same_v<T, protocol::CancelOrder>)
                return {request.client_id, request.request_id};
            return {};
        }, message);
        const auto fingerprint = protocol::encode(message);
        const auto prior = dedup_.find(key);
        if (prior != dedup_.end()) {
            if (prior->second.fingerprint == fingerprint) return {prior->second.response};
            const std::uint64_t order_id = std::holds_alternative<protocol::SubmitOrder>(message)
                ? std::get<protocol::SubmitOrder>(message).order_id : std::get<protocol::CancelOrder>(message).order_id;
            return {rejection(key.first, key.second, order_id, protocol::ResultCode::Duplicate)};
        }
        if (dedup_.size() >= config_.dedup_capacity) {
            const std::uint64_t order_id = std::holds_alternative<protocol::SubmitOrder>(message)
                ? std::get<protocol::SubmitOrder>(message).order_id : std::get<protocol::CancelOrder>(message).order_id;
            return {rejection(key.first, key.second, order_id, protocol::ResultCode::InternalError)};
        }

        std::vector<Message> output;
        if (const auto* request = std::get_if<protocol::SubmitOrder>(&message)) {
            if (order_records_ >= config_.max_order_records) {
                output.emplace_back(rejection(request->client_id, request->request_id, request->order_id,
                                              protocol::ResultCode::InternalError));
            } else {
                OrderRequest engine_request{request->order_id, request->client_id, request->request_id,
                    request->instrument_id,
                    request->side == protocol::Side::Buy ? Side::Buy : Side::Sell,
                    request->order_type == protocol::OrderType::Limit ? OrderType::Limit : OrderType::Market,
                    request->limit_price, request->quantity};
                const SubmitResult result = book_.submit(engine_request);
                if (!result.accepted() || !result.order) {
                    output.emplace_back(rejection(request->client_id, request->request_id, request->order_id,
                                                  submit_result_code(result.error)));
                } else {
                    ++order_records_;
                    for (const Trade& trade : result.trades) {
                        output.emplace_back(protocol::TradeEvent{trade.trade_sequence, trade.command_sequence,
                            trade.instrument_id, trade.maker_order_id, trade.taker_order_id,
                            trade.maker_client_id, trade.taker_client_id,
                            trade.taker_side == Side::Buy ? protocol::Side::Buy : protocol::Side::Sell,
                            trade.price, trade.quantity});
                    }
                    const OrderState& state = *result.order;
                    output.emplace_back(protocol::OrderResponse{request->client_id, request->request_id,
                        request->order_id, state.command_sequence, protocol_status(state.status),
                        protocol::ResultCode::Accepted, state.filled_quantity, state.leaves_quantity,
                        state.canceled_quantity});
                }
            }
        } else {
            const auto& cancel_request = std::get<protocol::CancelOrder>(message);
            const auto existing = book_.find_order(cancel_request.order_id);
            if (!existing || existing->request.client_id != cancel_request.client_id) {
                output.emplace_back(rejection(cancel_request.client_id, cancel_request.request_id, cancel_request.order_id,
                                              protocol::ResultCode::NotFound));
            } else {
                const CancelResult result = book_.cancel(cancel_request.order_id);
                if (!result.canceled()) {
                    output.emplace_back(rejection(cancel_request.client_id, cancel_request.request_id, cancel_request.order_id,
                                                  result.error == CancelError::UnknownOrderId
                                                      ? protocol::ResultCode::NotFound
                                                      : protocol::ResultCode::NotLive));
                } else {
                    const auto state = book_.find_order(cancel_request.order_id);
                    output.emplace_back(protocol::OrderResponse{cancel_request.client_id, cancel_request.request_id,
                        cancel_request.order_id, result.command_sequence, protocol::OrderStatus::Canceled,
                        protocol::ResultCode::Accepted, state->filled_quantity, 0,
                        state->canceled_quantity});
                }
            }
        }
        const auto response = std::find_if(output.begin(), output.end(), [](const Message& item) {
            return std::holds_alternative<protocol::OrderResponse>(item);
        });
        if (response != output.end()) {
            dedup_.emplace(key, DedupEntry{fingerprint, std::get<protocol::OrderResponse>(*response)});
        }
        return output;
    }

    NodeConfig config_;
    BoundedCommandQueue queue_;
    std::atomic<int> listen_fd_{-1};
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};
    std::uint16_t bound_port_{};
    std::thread accept_thread_;
    std::thread execution_thread_;
    std::vector<std::thread> transport_threads_;
    std::mutex transport_mutex_;
    std::condition_variable transport_ready_;
    std::map<std::uint64_t, std::deque<Message>> peer_outbound_;
    bool transport_stopping_{false};
    std::mutex clients_mutex_;
    std::set<int> client_fds_;
    OrderBook book_; // accessed only by execution_thread_
    std::map<RequestKey, DedupEntry> dedup_; // accessed only by execution_thread_
    std::size_t order_records_{0}; // accepted orders retained for this process lifetime
    std::unique_ptr<raft::FilePersistentStorage> raft_storage_;
    std::unique_ptr<ReplicatedExchangeStateMachine> state_machine_;
    std::unique_ptr<SystemRaftClock> raft_clock_;
    std::unique_ptr<RandomElectionTimeout> timeout_source_;
    std::unique_ptr<raft::RaftNode> raft_node_; // accessed only by execution_thread_
    std::map<RequestKey, PendingRequest> pending_requests_;
    std::map<raft::LogIndex, RequestKey> requests_by_index_;
    std::map<RequestKey, std::vector<std::uint8_t>> dedup_fingerprints_;
    std::atomic<bool> replicated_failed_{false};
    std::atomic<std::uint64_t> published_term_{0};
    std::atomic<std::uint8_t> published_role_{0};
    std::atomic<std::uint64_t> published_commit_{0};
    std::atomic<std::uint64_t> published_applied_{0};
};

TcpNode::TcpNode(NodeConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {}
TcpNode::~TcpNode() = default;
void TcpNode::start() { impl_->start(); }
void TcpNode::stop() noexcept { impl_->stop(); }
std::uint16_t TcpNode::port() const noexcept { return impl_->port(); }
std::uint64_t TcpNode::node_id() const noexcept { return impl_->node_id(); }
bool TcpNode::replicated_mode() const noexcept { return impl_->replicated_mode(); }
NodeRaftStatus TcpNode::raft_status() const noexcept { return impl_->raft_status(); }
OrderBookState TcpNode::local_state(std::chrono::milliseconds timeout) { return impl_->local_state(timeout); }

} // namespace atlas
