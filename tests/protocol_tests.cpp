#include "atlas/protocol.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace {
using namespace atlas::protocol;

void require(bool value, const std::string& message) {
    if (!value) throw std::runtime_error(message);
}

template <typename F>
void require_protocol_error(F&& operation, const std::string& label) {
    bool rejected = false;
    try { operation(); } catch (const ProtocolError&) { rejected = true; }
    require(rejected, label + " rejected");
}

void round_trip(const Message& message, const std::string& label) {
    const auto frame = encode(message);
    const Message decoded = decode(frame);
    const auto reencoded = encode(decoded);
    require(frame == reencoded, label + " round trips byte-for-byte");
    require(frame.size() >= kLengthPrefixBytes + kFrameHeaderBytes, label + " has complete frame");
    const std::uint32_t body = (static_cast<std::uint32_t>(frame[0]) << 24) |
                               (static_cast<std::uint32_t>(frame[1]) << 16) |
                               (static_cast<std::uint32_t>(frame[2]) << 8) | frame[3];
    require(body == frame.size() - kLengthPrefixBytes, label + " uses network byte order length");
}

void test_all_message_types() {
    round_trip(SubmitOrder{10, 20, 30, 40, Side::Sell, OrderType::Limit, 123456, 99}, "submit");
    round_trip(CancelOrder{10, 21, 30}, "cancel");
    round_trip(OrderResponse{10, 20, 30, 7, OrderStatus::PartiallyFilled,
                             ResultCode::Accepted, 2, 3, 0}, "order response");
    round_trip(OrderResponse{10, 21, 30, 0, OrderStatus::Rejected,
                             ResultCode::NotLeader, 0, 0, 0}, "not-leader response");
    round_trip(TradeEvent{8, 7, 40, 30, 31, 10, 11, Side::Buy, 123456, 2}, "trade event");
    round_trip(Heartbeat{10, 22, 0xabcdef, HeartbeatKind::Ping}, "heartbeat ping");
    round_trip(Heartbeat{10, 22, 0xabcdef, HeartbeatKind::Pong}, "heartbeat pong");

    const auto frame = encode(SubmitOrder{1, 2, 3, 4, Side::Buy, OrderType::Limit, 5, 6});
    require(frame[4] == kVersion && frame[5] == static_cast<std::uint8_t>(MessageType::SubmitOrder),
            "version and type fields are explicit");
    require(frame[6] == 0 && frame[13] == 1, "integer fields use big-endian encoding");
}

void test_invalid_fields_rejected() {
    require_protocol_error([] { (void)encode(SubmitOrder{0, 1, 2, 3, Side::Buy, OrderType::Limit, 1, 1}); }, "zero client ID");
    require_protocol_error([] { (void)encode(SubmitOrder{1, 0, 2, 3, Side::Buy, OrderType::Limit, 1, 1}); }, "zero request ID");
    require_protocol_error([] { (void)encode(SubmitOrder{1, 1, 2, 3, static_cast<Side>(0), OrderType::Limit, 1, 1}); }, "invalid side");
    require_protocol_error([] { (void)encode(SubmitOrder{1, 1, 2, 3, Side::Buy, OrderType::Limit, 0, 1}); }, "invalid price");
    require_protocol_error([] { (void)encode(SubmitOrder{1, 1, 2, 3, Side::Buy, OrderType::Limit, 1, 0}); }, "invalid quantity");
    require_protocol_error([] { (void)encode(Heartbeat{1, 2, 3, static_cast<HeartbeatKind>(9)}); }, "invalid heartbeat type");
}

void test_bad_frames_rejected() {
    const auto good = encode(CancelOrder{7, 8, 9});
    auto truncated = good;
    truncated.pop_back();
    require_protocol_error([&] { (void)decode(truncated); }, "truncated payload");
    auto trailing = good;
    trailing.push_back(0);
    require_protocol_error([&] { (void)decode(trailing); }, "trailing bytes");
    auto version = good;
    version[4] = 2;
    require_protocol_error([&] { (void)decode(version); }, "unsupported version");
    auto type = good;
    type[5] = 99;
    require_protocol_error([&] { (void)decode(type); }, "unknown message type");
    auto invalid_len = good;
    invalid_len[0] = 0;
    invalid_len[1] = 0;
    invalid_len[2] = 0;
    invalid_len[3] = 1;
    require_protocol_error([&] { (void)decode(invalid_len); }, "too-small declared frame");
    auto oversized = good;
    const std::uint32_t too_big = static_cast<std::uint32_t>(kMaxFrameBytes + 1);
    oversized[0] = static_cast<std::uint8_t>(too_big >> 24);
    oversized[1] = static_cast<std::uint8_t>(too_big >> 16);
    oversized[2] = static_cast<std::uint8_t>(too_big >> 8);
    oversized[3] = static_cast<std::uint8_t>(too_big);
    require_protocol_error([&] { (void)decode(oversized); }, "oversized declared frame");
    require_protocol_error([] { (void)decode({0, 0, 0}); }, "truncated length prefix");
}

void test_partial_stream_io() {
    const Message expected = SubmitOrder{101, 202, 303, 404, Side::Buy, OrderType::Limit, 505, 606};
    const auto frame = encode(expected);
    std::size_t cursor = 0;
    const Message received = read_message([&](std::uint8_t* destination, std::size_t requested) -> std::ptrdiff_t {
        const std::size_t count = std::min<std::size_t>({requested, 3, frame.size() - cursor});
        std::copy_n(frame.data() + cursor, count, destination);
        cursor += count;
        return static_cast<std::ptrdiff_t>(count);
    });
    require(encode(received) == frame && cursor == frame.size(), "reader joins multiple short reads");

    std::vector<std::uint8_t> output;
    write_message(expected, [&](const std::uint8_t* source, std::size_t requested) -> std::ptrdiff_t {
        const std::size_t count = std::min<std::size_t>(requested, 2);
        output.insert(output.end(), source, source + count);
        return static_cast<std::ptrdiff_t>(count);
    });
    require(output == frame, "writer loops across multiple short writes");

    require_protocol_error([] {
        (void)read_message([](std::uint8_t*, std::size_t) -> std::ptrdiff_t { return 0; });
    }, "EOF during stream read");
    require_protocol_error([&] {
        write_message(expected, [](const std::uint8_t*, std::size_t) -> std::ptrdiff_t { return 0; });
    }, "zero-progress stream write");
    require_protocol_error([&] {
        std::size_t count = 0;
        (void)read_message([&](std::uint8_t* destination, std::size_t requested) -> std::ptrdiff_t {
            if (count++ == 0) {
                const std::uint32_t too_big = static_cast<std::uint32_t>(kMaxFrameBytes + 1);
                destination[0] = static_cast<std::uint8_t>(too_big >> 24);
                destination[1] = static_cast<std::uint8_t>(too_big >> 16);
                destination[2] = static_cast<std::uint8_t>(too_big >> 8);
                destination[3] = static_cast<std::uint8_t>(too_big);
                return static_cast<std::ptrdiff_t>(std::min<std::size_t>(requested, 4));
            }
            return 0;
        });
    }, "oversized network frame rejected before body read");
}

} // namespace

int main() {
    try {
        test_all_message_types();
        test_invalid_fields_rejected();
        test_bad_frames_rejected();
        test_partial_stream_io();
        std::cout << "atlas_protocol_tests: all protocol tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas_protocol_tests: " << error.what() << '\n';
        return 1;
    }
}
