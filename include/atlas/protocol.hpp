#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <variant>
#include <vector>

namespace atlas::protocol {

inline constexpr std::uint8_t kVersion = 1;
inline constexpr std::size_t kLengthPrefixBytes = 4;
inline constexpr std::size_t kFrameHeaderBytes = 2; // version + type
inline constexpr std::size_t kMaxFrameBytes = 4096; // length prefix excluded

enum class MessageType : std::uint8_t {
    SubmitOrder = 1,
    CancelOrder = 2,
    OrderResponse = 3,
    TradeEvent = 4,
    Heartbeat = 5,
    RaftRequestVote = 6,
    RaftRequestVoteResponse = 7,
    RaftAppendEntries = 8,
    RaftAppendEntriesResponse = 9,
    RaftTransportEnvelope = 10,
};

enum class Side : std::uint8_t { Buy = 1, Sell = 2 };
enum class OrderType : std::uint8_t { Limit = 1 };
enum class OrderStatus : std::uint8_t { Open = 1, PartiallyFilled = 2, Filled = 3, Canceled = 4, Rejected = 5 };
enum class ResultCode : std::uint16_t {
    Accepted = 0, Invalid = 1, Duplicate = 2, NotFound = 3, NotLive = 4,
    InternalError = 5, NotLeader = 6
};
enum class HeartbeatKind : std::uint8_t { Ping = 1, Pong = 2 };

struct SubmitOrder {
    std::uint64_t client_id{};
    std::uint64_t request_id{};
    std::uint64_t order_id{};
    std::uint64_t instrument_id{};
    Side side{Side::Buy};
    OrderType order_type{OrderType::Limit};
    std::int64_t limit_price{};
    std::uint64_t quantity{};
};

struct CancelOrder {
    std::uint64_t client_id{};
    std::uint64_t request_id{};
    std::uint64_t order_id{};
};

struct OrderResponse {
    std::uint64_t client_id{};
    std::uint64_t request_id{};
    std::uint64_t order_id{};
    std::uint64_t command_sequence{};
    OrderStatus status{OrderStatus::Rejected};
    ResultCode result{ResultCode::Accepted};
    std::uint64_t filled_quantity{};
    std::uint64_t leaves_quantity{};
    std::uint64_t canceled_quantity{};
};

struct TradeEvent {
    std::uint64_t trade_sequence{};
    std::uint64_t command_sequence{};
    std::uint64_t instrument_id{};
    std::uint64_t maker_order_id{};
    std::uint64_t taker_order_id{};
    std::uint64_t maker_client_id{};
    std::uint64_t taker_client_id{};
    Side taker_side{Side::Buy};
    std::int64_t price{};
    std::uint64_t quantity{};
};

struct Heartbeat {
    std::uint64_t client_id{};
    std::uint64_t request_id{};
    std::uint64_t nonce{};
    HeartbeatKind kind{HeartbeatKind::Ping};
};

struct RaftRequestVote {
    std::uint64_t term{};
    std::uint64_t candidate_id{};
    std::uint64_t last_log_index{};
    std::uint64_t last_log_term{};
};
struct RaftRequestVoteResponse {
    std::uint64_t term{};
    bool vote_granted{};
};
struct RaftLogEntry {
    std::uint64_t term{};
    std::vector<std::uint8_t> command;
    friend bool operator==(const RaftLogEntry&, const RaftLogEntry&) = default;
};
struct RaftAppendEntries {
    std::uint64_t term{};
    std::uint64_t leader_id{};
    std::uint64_t prev_log_index{};
    std::uint64_t prev_log_term{};
    std::vector<RaftLogEntry> entries;
    std::uint64_t leader_commit{};
};
struct RaftAppendEntriesResponse {
    std::uint64_t term{};
    bool success{};
    std::uint64_t match_index{};
    std::uint64_t conflict_index{};
    std::uint64_t conflict_term{};
};
struct RaftTransportEnvelope {
    std::uint64_t sender_id{};
    std::vector<std::uint8_t> rpc_frame;
};

using Message = std::variant<SubmitOrder, CancelOrder, OrderResponse, TradeEvent, Heartbeat,
                             RaftRequestVote, RaftRequestVoteResponse,
                             RaftAppendEntries, RaftAppendEntriesResponse,
                             RaftTransportEnvelope>;

class ProtocolError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Encodes one complete frame. The four-byte big-endian length counts version, type, and payload.
[[nodiscard]] std::vector<std::uint8_t> encode(const Message& message);
// Decodes exactly one complete frame; trailing bytes and truncation are errors.
[[nodiscard]] Message decode(const std::vector<std::uint8_t>& frame);

// Stream callbacks return bytes transferred, 0 for EOF/no progress, and -1 for an I/O error.
// These helpers loop across short transfers and do not depend on sockets or an engine.
using ReadSome = std::function<std::ptrdiff_t(std::uint8_t*, std::size_t)>;
using WriteSome = std::function<std::ptrdiff_t(const std::uint8_t*, std::size_t)>;
[[nodiscard]] Message read_message(const ReadSome& read_some);
void write_message(const Message& message, const WriteSome& write_some);

} // namespace atlas::protocol
