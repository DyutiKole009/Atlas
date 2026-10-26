#include "atlas/storage.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using namespace atlas;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void require_trade_equal(const Trade& actual, const Trade& expected, const std::string& label) {
    require(actual.trade_sequence == expected.trade_sequence, label + " trade_sequence");
    require(actual.command_sequence == expected.command_sequence, label + " command_sequence");
    require(actual.instrument_id == expected.instrument_id, label + " instrument_id");
    require(actual.maker_order_id == expected.maker_order_id, label + " maker_order_id");
    require(actual.taker_order_id == expected.taker_order_id, label + " taker_order_id");
    require(actual.maker_client_id == expected.maker_client_id, label + " maker_client_id");
    require(actual.taker_client_id == expected.taker_client_id, label + " taker_client_id");
    require(actual.taker_side == expected.taker_side, label + " taker_side");
    require(actual.price == expected.price, label + " price");
    require(actual.quantity == expected.quantity, label + " quantity");
}

std::filesystem::path unique_path(const std::string& tag) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto file = std::filesystem::temp_directory_path() /
                      (std::string("atlas_wal_") + tag + "_" + std::to_string(now) + ".log");
    return file;
}

void test_wal_round_trip() {
    const auto path = unique_path("roundtrip");
    std::filesystem::remove(path);

    OrderBook original;
    std::vector<Trade> expected_trades;

    const auto bid = original.submit(OrderRequest{1, 1, 1, 1, Side::Buy, OrderType::Limit, 100, 5});
    require(bid.accepted(), "initial bid accepted");
    for (const auto& trade : bid.trades) {
        expected_trades.push_back(trade);
    }

    const auto ask = original.submit(OrderRequest{2, 2, 2, 1, Side::Sell, OrderType::Limit, 101, 3});
    require(ask.accepted(), "initial ask accepted");
    for (const auto& trade : ask.trades) {
        expected_trades.push_back(trade);
    }

    const auto cancel = original.cancel(2);
    require(cancel.canceled(), "cancel accepted");

    WriteAheadLog wal(path, WalDurabilityMode::Durable);
    const auto bid_append = wal.append_submit(bid.order->request, bid.order->command_sequence);
    const auto ask_append = wal.append_submit(ask.order->request, ask.order->command_sequence);
    const auto cancel_append = wal.append_cancel(cancel.order_id, cancel.command_sequence);
    require(bid_append.acknowledged && ask_append.acknowledged && cancel_append.acknowledged,
            "all WAL appends acknowledged");
    wal.flush();

    OrderBook replayed;
    const auto replay_summary = WriteAheadLog::replay(path, replayed);
    require(replay_summary.accepted_commands == 2, "two accepted submits replayed");
    require(replay_summary.canceled_commands == 1, "one cancel replayed");
    require(replay_summary.trades.size() == expected_trades.size(), "trade count matches");
    for (std::size_t index = 0; index < expected_trades.size(); ++index) {
        require_trade_equal(replay_summary.trades[index], expected_trades[index],
                            "replayed trade " + std::to_string(index));
    }
    require(replayed.invariants_hold(), "replayed book invariants hold");
    const auto original_snapshot = original.snapshot();
    const auto replayed_snapshot = replayed.snapshot();
    require(original_snapshot.bids.size() == replayed_snapshot.bids.size(), "bid level count matches");
    require(original_snapshot.asks.size() == replayed_snapshot.asks.size(), "ask level count matches");
    for (std::size_t level = 0; level < original_snapshot.bids.size(); ++level) {
        require(original_snapshot.bids[level].price == replayed_snapshot.bids[level].price,
                "bid price level matches");
        require(original_snapshot.bids[level].order_ids == replayed_snapshot.bids[level].order_ids,
                "bid order IDs match");
    }
    for (std::size_t level = 0; level < original_snapshot.asks.size(); ++level) {
        require(original_snapshot.asks[level].price == replayed_snapshot.asks[level].price,
                "ask price level matches");
        require(original_snapshot.asks[level].order_ids == replayed_snapshot.asks[level].order_ids,
                "ask order IDs match");
    }
}

void test_wal_replay_rejects_truncated_record() {
    const auto path = unique_path("truncated");
    std::filesystem::remove(path);

    OrderBook source;
    const auto accepted = source.submit(OrderRequest{10, 1, 10, 1, Side::Buy, OrderType::Limit, 100, 5});
    require(accepted.accepted(), "source order accepted");

    WriteAheadLog wal(path, WalDurabilityMode::Buffered);
    const auto append = wal.append_submit(accepted.order->request, accepted.order->command_sequence);
    require(append.acknowledged, "buffered append acknowledged");
    wal.flush();

    std::vector<std::uint8_t> bytes;
    std::ifstream in(path, std::ios::binary);
    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    in.seekg(0, std::ios::beg);
    bytes.resize(static_cast<std::size_t>(size));
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    bytes.resize(bytes.size() - 2);

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    out.flush();

    OrderBook replayed;
    bool threw = false;
    try {
        WriteAheadLog::replay(path, replayed);
    } catch (const WalError&) {
        threw = true;
    }
    require(threw, "truncated record is rejected");
}

void test_wal_replay_rejects_corruption() {
    const auto path = unique_path("corrupt");
    std::filesystem::remove(path);

    OrderBook source;
    const auto accepted = source.submit(OrderRequest{20, 1, 20, 1, Side::Buy, OrderType::Limit, 200, 5});
    require(accepted.accepted(), "source first order accepted");

    WriteAheadLog wal(path, WalDurabilityMode::Durable);
    const auto append = wal.append_submit(accepted.order->request, accepted.order->command_sequence);
    require(append.acknowledged, "durable append acknowledged");
    wal.flush();

    std::vector<std::uint8_t> bytes;
    std::ifstream in(path, std::ios::binary);
    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    in.seekg(0, std::ios::beg);
    bytes.resize(static_cast<std::size_t>(size));
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));

    if (!bytes.empty()) {
        bytes[bytes.size() - 1] ^= 0xFFU;
    }

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    out.flush();

    OrderBook replayed;
    bool threw = false;
    try {
        WriteAheadLog::replay(path, replayed);
    } catch (const WalError&) {
        threw = true;
    }
    require(threw, "checksum corruption is rejected");
}

} // namespace

int main() {
    try {
        test_wal_round_trip();
        test_wal_replay_rejects_truncated_record();
        test_wal_replay_rejects_corruption();
        std::cout << "atlas_wal_tests: all WAL tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas_wal_tests: " << error.what() << '\n';
        return 1;
    }
}
