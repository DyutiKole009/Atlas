#pragma once

#include "atlas/order_book.hpp"
#include "atlas/protocol.hpp"
#include "atlas/raft.hpp"
#include "atlas/raft_storage.hpp"

#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>

namespace atlas {

struct ReplicatedCommandResult {
    std::vector<protocol::Message> messages;
};

// Deterministic state machine for encoded v1 SubmitOrder and CancelOrder frames.
// Construct fresh at startup; Raft replays its durable committed prefix in index order.
class ReplicatedExchangeStateMachine final : public raft::StateMachine {
public:
    explicit ReplicatedExchangeStateMachine(raft::PersistentStorage& storage);
    void apply(raft::LogIndex index, const raft::LogEntry& entry) override;
    void restore_committed(const std::vector<raft::LogEntry>& entries) override;
    [[nodiscard]] std::optional<ReplicatedCommandResult> result(std::uint64_t client_id,
                                                                 std::uint64_t request_id) const;
    // The complete result for the original committed request; retries use result().
    [[nodiscard]] std::optional<ReplicatedCommandResult> take_applied_result(raft::LogIndex index);
    [[nodiscard]] const OrderBook& book() const noexcept { return book_; }
    [[nodiscard]] OrderBook& book() noexcept { return book_; }
    [[nodiscard]] raft::LogIndex applied_index() const noexcept { return applied_index_; }

private:
    struct RequestKey {
        std::uint64_t client_id{};
        std::uint64_t request_id{};
        friend bool operator<(const RequestKey& lhs, const RequestKey& rhs) noexcept {
            return lhs.client_id < rhs.client_id ||
                   (lhs.client_id == rhs.client_id && lhs.request_id < rhs.request_id);
        }
    };
    struct CachedResult {
        std::vector<std::uint8_t> encoded_request;
        ReplicatedCommandResult result;
    };

    OrderBook book_;
    raft::PersistentStorage& storage_;
    raft::LogIndex applied_index_{};
    std::map<RequestKey, CachedResult> results_;
    std::set<RequestKey> applied_request_keys_;
    std::map<raft::LogIndex, ReplicatedCommandResult> applied_results_;
};

[[nodiscard]] std::vector<std::uint8_t> encode_replicated_command(const protocol::Message& request);

} // namespace atlas
