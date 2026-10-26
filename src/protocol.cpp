#include "atlas/protocol.hpp"

#include <bit>
#include <limits>
#include <string>
#include <type_traits>

namespace atlas::protocol {
namespace {

void append_u8(std::vector<std::uint8_t>& out, std::uint8_t value) { out.push_back(value); }

void append_u16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    out.push_back(static_cast<std::uint8_t>(value >> 8));
    out.push_back(static_cast<std::uint8_t>(value));
}

void append_u32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>(value >> 24));
    out.push_back(static_cast<std::uint8_t>(value >> 16));
    out.push_back(static_cast<std::uint8_t>(value >> 8));
    out.push_back(static_cast<std::uint8_t>(value));
}

void append_u64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::uint8_t>(value >> shift));
    }
}

std::uint8_t read_u8(const std::vector<std::uint8_t>& bytes, std::size_t& offset) {
    return bytes[offset++];
}

std::uint16_t read_u16(const std::vector<std::uint8_t>& bytes, std::size_t& offset) {
    const auto result = static_cast<std::uint16_t>(bytes[offset]) << 8 |
                        static_cast<std::uint16_t>(bytes[offset + 1]);
    offset += 2;
    return result;
}

std::uint32_t read_u32(const std::uint8_t* bytes) {
    return (static_cast<std::uint32_t>(bytes[0]) << 24) |
           (static_cast<std::uint32_t>(bytes[1]) << 16) |
           (static_cast<std::uint32_t>(bytes[2]) << 8) |
           static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64(const std::vector<std::uint8_t>& bytes, std::size_t& offset) {
    std::uint64_t result = 0;
    for (int index = 0; index < 8; ++index) {
        result = (result << 8) | bytes[offset++];
    }
    return result;
}

std::int64_t read_i64(const std::vector<std::uint8_t>& bytes, std::size_t& offset) {
    return std::bit_cast<std::int64_t>(read_u64(bytes, offset));
}

void require_nonzero(std::uint64_t value, const char* field) {
    if (value == 0) throw ProtocolError(std::string(field) + " must be nonzero");
}

void validate_side(Side side) {
    if (side != Side::Buy && side != Side::Sell) throw ProtocolError("invalid side value");
}

void validate_message(const SubmitOrder& message) {
    require_nonzero(message.client_id, "client_id");
    require_nonzero(message.request_id, "request_id");
    require_nonzero(message.order_id, "order_id");
    require_nonzero(message.instrument_id, "instrument_id");
    validate_side(message.side);
    if (message.order_type != OrderType::Limit) throw ProtocolError("unsupported order type");
    if (message.limit_price <= 0) throw ProtocolError("limit price must be positive");
    require_nonzero(message.quantity, "quantity");
}

void validate_message(const CancelOrder& message) {
    require_nonzero(message.client_id, "client_id");
    require_nonzero(message.request_id, "request_id");
    require_nonzero(message.order_id, "order_id");
}

void validate_message(const OrderResponse& message) {
    require_nonzero(message.client_id, "client_id");
    require_nonzero(message.request_id, "request_id");
    if (message.status < OrderStatus::Open || message.status > OrderStatus::Rejected)
        throw ProtocolError("invalid order status");
    if (message.result < ResultCode::Accepted || message.result > ResultCode::NotLeader)
        throw ProtocolError("invalid result code");
    if (message.result == ResultCode::Accepted) {
        require_nonzero(message.order_id, "order_id");
        require_nonzero(message.command_sequence, "command_sequence");
        if (message.status == OrderStatus::Rejected) throw ProtocolError("accepted response cannot be rejected");
        if (message.status == OrderStatus::Open && (message.filled_quantity != 0 || message.leaves_quantity == 0))
            throw ProtocolError("invalid open-order quantities");
        if (message.status == OrderStatus::PartiallyFilled &&
            (message.filled_quantity == 0 || message.leaves_quantity == 0))
            throw ProtocolError("invalid partially-filled quantities");
        if (message.status == OrderStatus::Filled &&
            (message.filled_quantity == 0 || message.leaves_quantity != 0 || message.canceled_quantity != 0))
            throw ProtocolError("invalid filled-order quantities");
        if (message.status == OrderStatus::Canceled &&
            (message.leaves_quantity != 0 || message.canceled_quantity == 0))
            throw ProtocolError("invalid canceled-order quantities");
    } else if (message.status != OrderStatus::Rejected) {
        throw ProtocolError("rejected result must have rejected status");
    }
}

void validate_message(const TradeEvent& message) {
    require_nonzero(message.trade_sequence, "trade_sequence");
    require_nonzero(message.command_sequence, "command_sequence");
    require_nonzero(message.instrument_id, "instrument_id");
    require_nonzero(message.maker_order_id, "maker_order_id");
    require_nonzero(message.taker_order_id, "taker_order_id");
    require_nonzero(message.maker_client_id, "maker_client_id");
    require_nonzero(message.taker_client_id, "taker_client_id");
    validate_side(message.taker_side);
    if (message.price <= 0) throw ProtocolError("trade price must be positive");
    require_nonzero(message.quantity, "quantity");
}

void validate_message(const Heartbeat& message) {
    require_nonzero(message.client_id, "client_id");
    require_nonzero(message.request_id, "request_id");
    if (message.kind != HeartbeatKind::Ping && message.kind != HeartbeatKind::Pong)
        throw ProtocolError("invalid heartbeat kind");
}

void validate_message(const RaftRequestVote& message) {
    require_nonzero(message.term, "term");
    require_nonzero(message.candidate_id, "candidate_id");
    if ((message.last_log_index == 0) != (message.last_log_term == 0) || message.last_log_term > message.term)
        throw ProtocolError("invalid Raft candidate log metadata");
}
void validate_message(const RaftRequestVoteResponse& message) {
    require_nonzero(message.term, "term");
}
void validate_message(const RaftAppendEntries& message) {
    require_nonzero(message.term, "term");
    require_nonzero(message.leader_id, "leader_id");
    if ((message.prev_log_index == 0) != (message.prev_log_term == 0) ||
        message.prev_log_term > message.term || message.entries.size() > 255)
        throw ProtocolError("invalid Raft AppendEntries metadata");
    std::size_t payload_bytes = 41;
    std::uint64_t previous_term = message.prev_log_term;
    for (const auto& entry : message.entries) {
        require_nonzero(entry.term, "entry term");
        if (entry.term > message.term || entry.term < previous_term || entry.command.size() > UINT16_MAX)
            throw ProtocolError("invalid Raft log entry");
        previous_term = entry.term;
        if (payload_bytes + 10 + entry.command.size() > kMaxFrameBytes - kFrameHeaderBytes)
            throw ProtocolError("Raft AppendEntries exceeds maximum frame size");
        payload_bytes += 10 + entry.command.size();
    }
}
void validate_message(const RaftAppendEntriesResponse& message) {
    require_nonzero(message.term, "term");
    if (message.success && message.conflict_index != 0)
        throw ProtocolError("successful AppendEntries response cannot contain conflict index");
}
void validate_message(const RaftTransportEnvelope& message) {
    require_nonzero(message.sender_id, "sender_id");
    if (message.rpc_frame.empty() || message.rpc_frame.size() > kMaxFrameBytes)
        throw ProtocolError("invalid nested Raft frame length");
}

MessageType type_of(const Message& message) {
    return std::visit([](const auto& value) -> MessageType {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, SubmitOrder>) return MessageType::SubmitOrder;
        if constexpr (std::is_same_v<T, CancelOrder>) return MessageType::CancelOrder;
        if constexpr (std::is_same_v<T, OrderResponse>) return MessageType::OrderResponse;
        if constexpr (std::is_same_v<T, TradeEvent>) return MessageType::TradeEvent;
        if constexpr (std::is_same_v<T, Heartbeat>) return MessageType::Heartbeat;
        if constexpr (std::is_same_v<T, RaftRequestVote>) return MessageType::RaftRequestVote;
        if constexpr (std::is_same_v<T, RaftRequestVoteResponse>) return MessageType::RaftRequestVoteResponse;
        if constexpr (std::is_same_v<T, RaftAppendEntries>) return MessageType::RaftAppendEntries;
        if constexpr (std::is_same_v<T, RaftAppendEntriesResponse>) return MessageType::RaftAppendEntriesResponse;
        return MessageType::RaftTransportEnvelope;
    }, message);
}

std::vector<std::uint8_t> payload_of(const Message& message) {
    return std::visit([](const auto& value) {
        validate_message(value);
        std::vector<std::uint8_t> payload;
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, SubmitOrder>) {
            append_u64(payload, value.client_id); append_u64(payload, value.request_id);
            append_u64(payload, value.order_id); append_u64(payload, value.instrument_id);
            append_u8(payload, static_cast<std::uint8_t>(value.side));
            append_u8(payload, static_cast<std::uint8_t>(value.order_type));
            append_u64(payload, std::bit_cast<std::uint64_t>(value.limit_price));
            append_u64(payload, value.quantity);
        } else if constexpr (std::is_same_v<T, CancelOrder>) {
            append_u64(payload, value.client_id); append_u64(payload, value.request_id);
            append_u64(payload, value.order_id);
        } else if constexpr (std::is_same_v<T, OrderResponse>) {
            append_u64(payload, value.client_id); append_u64(payload, value.request_id);
            append_u64(payload, value.order_id); append_u64(payload, value.command_sequence);
            append_u8(payload, static_cast<std::uint8_t>(value.status));
            append_u16(payload, static_cast<std::uint16_t>(value.result));
            append_u64(payload, value.filled_quantity); append_u64(payload, value.leaves_quantity);
            append_u64(payload, value.canceled_quantity);
        } else if constexpr (std::is_same_v<T, TradeEvent>) {
            append_u64(payload, value.trade_sequence); append_u64(payload, value.command_sequence);
            append_u64(payload, value.instrument_id); append_u64(payload, value.maker_order_id);
            append_u64(payload, value.taker_order_id); append_u64(payload, value.maker_client_id);
            append_u64(payload, value.taker_client_id); append_u8(payload, static_cast<std::uint8_t>(value.taker_side));
            append_u64(payload, std::bit_cast<std::uint64_t>(value.price)); append_u64(payload, value.quantity);
        } else if constexpr (std::is_same_v<T, Heartbeat>) {
            append_u64(payload, value.client_id); append_u64(payload, value.request_id);
            append_u64(payload, value.nonce); append_u8(payload, static_cast<std::uint8_t>(value.kind));
        } else if constexpr (std::is_same_v<T, RaftRequestVote>) {
            append_u64(payload, value.term); append_u64(payload, value.candidate_id);
            append_u64(payload, value.last_log_index); append_u64(payload, value.last_log_term);
        } else if constexpr (std::is_same_v<T, RaftRequestVoteResponse>) {
            append_u64(payload, value.term); append_u8(payload, value.vote_granted ? 1 : 0);
        } else if constexpr (std::is_same_v<T, RaftAppendEntries>) {
            append_u64(payload, value.term); append_u64(payload, value.leader_id);
            append_u64(payload, value.prev_log_index); append_u64(payload, value.prev_log_term);
            append_u64(payload, value.leader_commit);
            append_u8(payload, static_cast<std::uint8_t>(value.entries.size()));
            for (const auto& entry : value.entries) {
                append_u64(payload, entry.term);
                append_u16(payload, static_cast<std::uint16_t>(entry.command.size()));
                payload.insert(payload.end(), entry.command.begin(), entry.command.end());
            }
        } else if constexpr (std::is_same_v<T, RaftAppendEntriesResponse>) {
            append_u64(payload, value.term); append_u8(payload, value.success ? 1 : 0);
            append_u64(payload, value.match_index); append_u64(payload, value.conflict_index);
            append_u64(payload, value.conflict_term);
        } else {
            append_u64(payload, value.sender_id);
            if (value.rpc_frame.size() > UINT16_MAX) throw ProtocolError("nested Raft frame is too large");
            append_u16(payload, static_cast<std::uint16_t>(value.rpc_frame.size()));
            payload.insert(payload.end(), value.rpc_frame.begin(), value.rpc_frame.end());
        }
        return payload;
    }, message);
}

Message decode_payload(MessageType type, const std::vector<std::uint8_t>& payload) {
    std::size_t offset = 0;
    auto expect_size = [&](std::size_t size) {
        if (payload.size() != size) throw ProtocolError("message payload has invalid length");
    };
    switch (type) {
    case MessageType::SubmitOrder: {
        expect_size(50);
        SubmitOrder m;
        m.client_id = read_u64(payload, offset); m.request_id = read_u64(payload, offset);
        m.order_id = read_u64(payload, offset); m.instrument_id = read_u64(payload, offset);
        m.side = static_cast<Side>(read_u8(payload, offset));
        m.order_type = static_cast<OrderType>(read_u8(payload, offset));
        m.limit_price = read_i64(payload, offset); m.quantity = read_u64(payload, offset);
        validate_message(m); return m;
    }
    case MessageType::CancelOrder: {
        expect_size(24);
        CancelOrder m{read_u64(payload, offset), read_u64(payload, offset), read_u64(payload, offset)};
        validate_message(m); return m;
    }
    case MessageType::OrderResponse: {
        expect_size(59);
        OrderResponse m;
        m.client_id = read_u64(payload, offset); m.request_id = read_u64(payload, offset);
        m.order_id = read_u64(payload, offset); m.command_sequence = read_u64(payload, offset);
        m.status = static_cast<OrderStatus>(read_u8(payload, offset));
        m.result = static_cast<ResultCode>(read_u16(payload, offset));
        m.filled_quantity = read_u64(payload, offset); m.leaves_quantity = read_u64(payload, offset);
        m.canceled_quantity = read_u64(payload, offset);
        validate_message(m); return m;
    }
    case MessageType::TradeEvent: {
        expect_size(73);
        TradeEvent m;
        m.trade_sequence = read_u64(payload, offset); m.command_sequence = read_u64(payload, offset);
        m.instrument_id = read_u64(payload, offset); m.maker_order_id = read_u64(payload, offset);
        m.taker_order_id = read_u64(payload, offset); m.maker_client_id = read_u64(payload, offset);
        m.taker_client_id = read_u64(payload, offset); m.taker_side = static_cast<Side>(read_u8(payload, offset));
        m.price = read_i64(payload, offset); m.quantity = read_u64(payload, offset);
        validate_message(m); return m;
    }
    case MessageType::Heartbeat: {
        expect_size(25);
        Heartbeat m;
        m.client_id = read_u64(payload, offset); m.request_id = read_u64(payload, offset);
        m.nonce = read_u64(payload, offset); m.kind = static_cast<HeartbeatKind>(read_u8(payload, offset));
        validate_message(m); return m;
    }
    case MessageType::RaftRequestVote: {
        expect_size(32);
        RaftRequestVote m{read_u64(payload, offset), read_u64(payload, offset),
                          read_u64(payload, offset), read_u64(payload, offset)};
        validate_message(m); return m;
    }
    case MessageType::RaftRequestVoteResponse: {
        expect_size(9);
        RaftRequestVoteResponse m;
        m.term = read_u64(payload, offset);
        const auto granted = read_u8(payload, offset);
        if (granted > 1) throw ProtocolError("invalid Raft vote response flag");
        m.vote_granted = granted != 0;
        validate_message(m); return m;
    }
    case MessageType::RaftAppendEntries: {
        if (payload.size() < 41) throw ProtocolError("Raft AppendEntries payload is truncated");
        RaftAppendEntries m;
        m.term = read_u64(payload, offset); m.leader_id = read_u64(payload, offset);
        m.prev_log_index = read_u64(payload, offset); m.prev_log_term = read_u64(payload, offset);
        m.leader_commit = read_u64(payload, offset);
        const auto count = read_u8(payload, offset);
        m.entries.reserve(count);
        for (std::uint16_t i = 0; i < count; ++i) {
            if (offset + 10 > payload.size()) throw ProtocolError("Raft log entry header is truncated");
            RaftLogEntry entry;
            entry.term = read_u64(payload, offset);
            const auto length = read_u16(payload, offset);
            if (length > payload.size() - offset) throw ProtocolError("Raft command is truncated");
            entry.command.assign(payload.begin() + static_cast<std::ptrdiff_t>(offset),
                                 payload.begin() + static_cast<std::ptrdiff_t>(offset + length));
            offset += length;
            m.entries.push_back(std::move(entry));
        }
        if (offset != payload.size()) throw ProtocolError("trailing bytes in Raft AppendEntries");
        validate_message(m); return m;
    }
    case MessageType::RaftAppendEntriesResponse: {
        expect_size(33);
        RaftAppendEntriesResponse m;
        m.term = read_u64(payload, offset);
        const auto success = read_u8(payload, offset);
        if (success > 1) throw ProtocolError("invalid Raft AppendEntries response flag");
        m.success = success != 0;
        m.match_index = read_u64(payload, offset);
        m.conflict_index = read_u64(payload, offset);
        m.conflict_term = read_u64(payload, offset);
        validate_message(m); return m;
    }
    case MessageType::RaftTransportEnvelope: {
        if (payload.size() < 10) throw ProtocolError("Raft transport envelope is truncated");
        RaftTransportEnvelope m;
        m.sender_id = read_u64(payload, offset);
        const auto length = read_u16(payload, offset);
        if (length != payload.size() - offset) throw ProtocolError("Raft envelope nested length mismatch");
        m.rpc_frame.assign(payload.begin() + static_cast<std::ptrdiff_t>(offset), payload.end());
        validate_message(m);
        const Message nested = decode(m.rpc_frame);
        if (!std::holds_alternative<RaftRequestVote>(nested) &&
            !std::holds_alternative<RaftRequestVoteResponse>(nested) &&
            !std::holds_alternative<RaftAppendEntries>(nested) &&
            !std::holds_alternative<RaftAppendEntriesResponse>(nested))
            throw ProtocolError("envelope does not contain a Raft RPC");
        return m;
    }
    }
    throw ProtocolError("unknown message type");
}

void read_exact(const ReadSome& reader, std::uint8_t* data, std::size_t size) {
    if (!reader) throw ProtocolError("read callback is empty");
    std::size_t total = 0;
    while (total < size) {
        const std::ptrdiff_t count = reader(data + total, size - total);
        if (count < 0) throw ProtocolError("stream read failed");
        if (count == 0) throw ProtocolError("unexpected end of stream");
        if (static_cast<std::size_t>(count) > size - total) throw ProtocolError("read callback over-reported bytes");
        total += static_cast<std::size_t>(count);
    }
}

void write_all(const WriteSome& writer, const std::uint8_t* data, std::size_t size) {
    if (!writer) throw ProtocolError("write callback is empty");
    std::size_t total = 0;
    while (total < size) {
        const std::ptrdiff_t count = writer(data + total, size - total);
        if (count < 0) throw ProtocolError("stream write failed");
        if (count == 0) throw ProtocolError("stream write made no progress");
        if (static_cast<std::size_t>(count) > size - total) throw ProtocolError("write callback over-reported bytes");
        total += static_cast<std::size_t>(count);
    }
}

} // namespace

std::vector<std::uint8_t> encode(const Message& message) {
    const std::vector<std::uint8_t> payload = payload_of(message);
    const std::size_t body_length = kFrameHeaderBytes + payload.size();
    if (body_length > kMaxFrameBytes || body_length > std::numeric_limits<std::uint32_t>::max())
        throw ProtocolError("frame exceeds maximum size");
    std::vector<std::uint8_t> frame;
    frame.reserve(kLengthPrefixBytes + body_length);
    append_u32(frame, static_cast<std::uint32_t>(body_length));
    append_u8(frame, kVersion);
    append_u8(frame, static_cast<std::uint8_t>(type_of(message)));
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

Message decode(const std::vector<std::uint8_t>& frame) {
    if (frame.size() < kLengthPrefixBytes) throw ProtocolError("truncated frame length prefix");
    const std::uint32_t body_length = read_u32(frame.data());
    if (body_length < kFrameHeaderBytes || body_length > kMaxFrameBytes)
        throw ProtocolError("frame length is outside supported bounds");
    if (frame.size() != kLengthPrefixBytes + body_length)
        throw ProtocolError("frame length prefix does not match supplied bytes");
    if (frame[kLengthPrefixBytes] != kVersion) throw ProtocolError("unsupported protocol version");
    const auto type = static_cast<MessageType>(frame[kLengthPrefixBytes + 1]);
    if (type < MessageType::SubmitOrder || type > MessageType::RaftTransportEnvelope)
        throw ProtocolError("unknown message type");
    std::vector<std::uint8_t> payload(frame.begin() + static_cast<std::ptrdiff_t>(kLengthPrefixBytes + kFrameHeaderBytes), frame.end());
    return decode_payload(type, payload);
}

Message read_message(const ReadSome& read_some) {
    std::uint8_t prefix[kLengthPrefixBytes]{};
    read_exact(read_some, prefix, sizeof(prefix));
    const std::uint32_t body_length = read_u32(prefix);
    if (body_length < kFrameHeaderBytes || body_length > kMaxFrameBytes)
        throw ProtocolError("frame length is outside supported bounds");
    std::vector<std::uint8_t> frame(kLengthPrefixBytes + body_length);
    for (std::size_t i = 0; i < kLengthPrefixBytes; ++i) frame[i] = prefix[i];
    read_exact(read_some, frame.data() + kLengthPrefixBytes, body_length);
    return decode(frame);
}

void write_message(const Message& message, const WriteSome& write_some) {
    const std::vector<std::uint8_t> frame = encode(message);
    write_all(write_some, frame.data(), frame.size());
}

} // namespace atlas::protocol
