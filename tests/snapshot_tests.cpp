#include "atlas/storage.hpp"

#include <chrono>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {

using namespace atlas;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::filesystem::path unique_base(const std::string& tag) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("atlas_snapshot_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(now));
}

OrderRequest order(OrderId id, ClientId client, Side side, PriceTicks price, Quantity quantity) {
    return OrderRequest{id, client, id, 1, side, OrderType::Limit, price, quantity};
}

void submit_logged(OrderBook& book, WriteAheadLog& wal, const OrderRequest& request) {
    const auto result = book.submit(request);
    require(result.accepted() && result.order.has_value(), "logged submit accepted");
    const auto appended = wal.append_submit(request, result.order->command_sequence);
    require(appended.acknowledged, "logged submit acknowledged");
}

void cancel_logged(OrderBook& book, WriteAheadLog& wal, OrderId id) {
    const auto result = book.cancel(id);
    require(result.canceled(), "logged cancel accepted");
    const auto appended = wal.append_cancel(id, result.command_sequence);
    require(appended.acknowledged, "logged cancel acknowledged");
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

void require_same_state(const OrderBook& expected, const OrderBook& actual, const std::string& label) {
    const auto lhs = expected.export_state();
    const auto rhs = actual.export_state();
    require(lhs.instrument_id == rhs.instrument_id, label + " instrument");
    require(lhs.next_command_sequence == rhs.next_command_sequence, label + " command sequence");
    require(lhs.next_trade_sequence == rhs.next_trade_sequence, label + " trade sequence");
    require(lhs.orders.size() == rhs.orders.size(), label + " order count");
    for (std::size_t i = 0; i < lhs.orders.size(); ++i) {
        require(same_order(lhs.orders[i], rhs.orders[i]), label + " order state " + std::to_string(i));
    }
    const auto a = expected.snapshot();
    const auto b = actual.snapshot();
    const auto levels_equal = [](const auto& x, const auto& y) {
        if (x.size() != y.size()) return false;
        for (std::size_t i = 0; i < x.size(); ++i) {
            if (x[i].price != y[i].price || x[i].order_ids != y[i].order_ids) return false;
        }
        return true;
    };
    require(levels_equal(a.bids, b.bids) && levels_equal(a.asks, b.asks), label + " price-time queues");
    require(actual.invariants_hold(), label + " invariants");
}

std::vector<std::uint8_t> read_bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    input.seekg(0, std::ios::end);
    const auto end = input.tellg();
    require(input.good() && end >= 0, "read file size");
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(end));
    input.seekg(0, std::ios::beg);
    if (!bytes.empty()) input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    require(input.good() || input.eof(), "read file bytes");
    return bytes;
}

void write_bytes(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();
    require(output.good(), "write test fixture");
}

void test_empty_snapshot_and_incremental_replay() {
    const auto base = unique_base("empty");
    const auto wal_path = base.string() + ".wal";
    const auto snapshot_path = base.string() + ".snap";
    OrderBook source;
    WriteAheadLog wal(wal_path, WalDurabilityMode::Durable);
    const auto saved = SnapshotStore::save(snapshot_path, wal, source);
    require(saved.order_count == 0 && saved.wal_offset == 0, "empty checkpoint metadata");

    submit_logged(source, wal, order(1, 1, Side::Buy, 90, 3));
    submit_logged(source, wal, order(2, 2, Side::Buy, 90, 4));
    OrderBook recovered;
    const auto replay = SnapshotStore::recover(snapshot_path, wal_path, recovered);
    require(replay.accepted_commands == 2, "only post-snapshot commands replayed");
    require_same_state(source, recovered, "empty snapshot recovery");
    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);
}

void test_populated_snapshot_with_partial_fill_cancel_and_continuation() {
    const auto base = unique_base("populated");
    const auto wal_path = base.string() + ".wal";
    const auto snapshot_path = base.string() + ".snap";
    OrderBook source;
    WriteAheadLog wal(wal_path, WalDurabilityMode::Durable);
    submit_logged(source, wal, order(1, 1, Side::Sell, 100, 10));
    submit_logged(source, wal, order(2, 2, Side::Sell, 100, 4));
    submit_logged(source, wal, order(3, 3, Side::Buy, 100, 6));
    cancel_logged(source, wal, 2);
    submit_logged(source, wal, order(4, 4, Side::Buy, 90, 5));
    const auto saved = SnapshotStore::save(snapshot_path, wal, source);
    require(saved.order_count == 4 && saved.wal_offset > 0, "populated checkpoint metadata");

    submit_logged(source, wal, order(5, 5, Side::Buy, 90, 2));
    submit_logged(source, wal, order(6, 6, Side::Buy, 100, 6));
    const auto before_cancel = source.find_order(1);
    require(before_cancel && before_cancel->filled_quantity == 10, "post-checkpoint submit fills residual maker");
    cancel_logged(source, wal, 4);

    OrderBook recovered;
    const auto replay = SnapshotStore::recover(snapshot_path, wal_path, recovered);
    require(replay.accepted_commands == 2 && replay.canceled_commands == 1,
            "recovery replays only suffix submit/cancel records");
    require(replay.trades.size() == 1 && replay.trades.front().trade_sequence == 2,
            "trade sequence continues from snapshot");
    require_same_state(source, recovered, "populated snapshot recovery");
    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);
}

void test_large_order_book_snapshot() {
    const auto base = unique_base("large");
    const auto wal_path = base.string() + ".wal";
    const auto snapshot_path = base.string() + ".snap";
    OrderBook source;
    WriteAheadLog wal(wal_path, WalDurabilityMode::Durable);
    constexpr std::uint64_t count = 1200;
    for (std::uint64_t id = 1; id <= count; ++id) {
        const Side side = (id % 2 == 0) ? Side::Buy : Side::Sell;
        submit_logged(source, wal, order(id, id, side, side == Side::Buy ? 90 : 110, id % 9 + 1));
    }
    const auto saved = SnapshotStore::save(snapshot_path, wal, source);
    require(saved.order_count == count, "large snapshot stores all order history");
    OrderBook recovered;
    const auto replay = SnapshotStore::recover(snapshot_path, wal_path, recovered);
    require(replay.accepted_commands == 0, "large snapshot requires no WAL suffix replay");
    require_same_state(source, recovered, "large snapshot recovery");
    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);
}

void test_snapshot_rejects_truncation_version_and_corruption() {
    const auto base = unique_base("invalid");
    const auto wal_path = base.string() + ".wal";
    const auto snapshot_path = base.string() + ".snap";
    OrderBook source;
    WriteAheadLog wal(wal_path, WalDurabilityMode::Durable);
    submit_logged(source, wal, order(1, 1, Side::Buy, 90, 1));
    SnapshotStore::save(snapshot_path, wal, source);
    const auto valid = read_bytes(snapshot_path);

    auto assert_rejected = [&](std::vector<std::uint8_t> bytes, const std::string& label) {
        write_bytes(snapshot_path, bytes);
        OrderBook target;
        bool rejected = false;
        try { (void)SnapshotStore::load(snapshot_path, target); }
        catch (const WalError&) { rejected = true; }
        require(rejected, label + " rejected");
        require(target.export_state().orders.empty(), label + " leaves target unchanged");
    };
    auto truncated = valid;
    truncated.pop_back();
    assert_rejected(std::move(truncated), "truncated snapshot");
    auto unsupported = valid;
    unsupported[8] = 2;
    assert_rejected(std::move(unsupported), "unsupported version");
    auto corrupt = valid;
    corrupt[corrupt.size() / 2] ^= 0x40U;
    assert_rejected(std::move(corrupt), "checksum corruption");
    auto bad_length = valid;
    bad_length[12] ^= 0x01U;
    assert_rejected(std::move(bad_length), "invalid payload length");
    write_bytes(snapshot_path, valid);
    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);
}

void test_incomplete_publication_and_mismatched_state() {
    const auto base = unique_base("publication");
    const auto wal_path = base.string() + ".wal";
    const auto snapshot_path = base.string() + ".snap";
    OrderBook source;
    WriteAheadLog wal(wal_path, WalDurabilityMode::Durable);
    submit_logged(source, wal, order(1, 1, Side::Buy, 90, 1));
    // Terminate a child after writing a partial temporary file but before publication.
    const auto unpublished_temp = base.string() + ".snap.tmp.abandoned";
    const pid_t child = ::fork();
    require(child >= 0, "fork incomplete-publication child");
    if (child == 0) {
        const int fd = ::open(unpublished_temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd < 0) ::_exit(20);
        const std::uint8_t partial[] = {'A', 'T', 'L', 'S'};
        if (::write(fd, partial, sizeof(partial)) != static_cast<ssize_t>(sizeof(partial))) ::_exit(21);
        ::_exit(0);
    }
    int child_status = 0;
    require(::waitpid(child, &child_status, 0) == child && WIFEXITED(child_status) &&
                WEXITSTATUS(child_status) == 0,
            "child terminated with unpublished partial snapshot");
    OrderBook recovered;
    const auto replay = SnapshotStore::recover(snapshot_path, wal_path, recovered);
    require(replay.accepted_commands == 1, "missing published snapshot falls back to full WAL");
    require_same_state(source, recovered, "recovery after interrupted publication");

    OrderBook stale_engine;
    const auto mismatch_path = base.string() + ".mismatch.snap";
    bool mismatch_rejected = false;
    try { (void)SnapshotStore::save(mismatch_path, wal, stale_engine); }
    catch (const WalError&) { mismatch_rejected = true; }
    require(mismatch_rejected, "checkpoint cannot skip durable WAL commands absent from engine state");
    std::filesystem::remove(unpublished_temp);
    std::filesystem::remove(wal_path);
}

void test_durable_ack_survives_process_termination() {
    const auto base = unique_base("process_crash");
    const auto wal_path = base.string() + ".wal";
    const pid_t child = ::fork();
    require(child >= 0, "fork child process");
    if (child == 0) {
        try {
            OrderBook book;
            const auto result = book.submit(order(42, 7, Side::Buy, 77, 3));
            if (!result.accepted() || !result.order) ::_exit(10);
            WriteAheadLog wal(wal_path, WalDurabilityMode::Durable);
            const auto append = wal.append_submit(result.order->request, result.order->command_sequence);
            if (!append.acknowledged) ::_exit(11);
            ::_exit(0); // Deliberately bypass destructors to model abrupt process termination.
        } catch (...) {
            ::_exit(12);
        }
    }
    int status = 0;
    require(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
            "child reached durable WAL acknowledgement before termination");
    OrderBook recovered;
    const auto replay = SnapshotStore::recover(base.string() + ".snap", wal_path, recovered);
    require(replay.accepted_commands == 1 && recovered.find_order(42).has_value(),
            "durably acknowledged command recovered after process termination");
    require(recovered.invariants_hold(), "process-crash recovery invariants");
    std::filesystem::remove(wal_path);
}

} // namespace

int main() {
    try {
        test_empty_snapshot_and_incremental_replay();
        test_populated_snapshot_with_partial_fill_cancel_and_continuation();
        test_large_order_book_snapshot();
        test_snapshot_rejects_truncation_version_and_corruption();
        test_incomplete_publication_and_mismatched_state();
        test_durable_ack_survives_process_termination();
        std::cout << "atlas_snapshot_tests: all snapshot and recovery tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas_snapshot_tests: " << error.what() << '\n';
        return 1;
    }
}
