#include "atlas/order_book.hpp"
#include "atlas/protocol.hpp"
#include "atlas/raft.hpp"
#include "atlas/raft_storage.hpp"
#include "atlas/storage.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <sys/resource.h>
#include <unistd.h>

#ifndef ATLAS_COMPILER_ID
#define ATLAS_COMPILER_ID "unknown"
#endif
#ifndef ATLAS_COMPILER_VERSION
#define ATLAS_COMPILER_VERSION "unknown"
#endif
#ifndef ATLAS_BUILD_MODE
#define ATLAS_BUILD_MODE "unknown"
#endif

namespace {
using namespace atlas;
using Clock = std::chrono::steady_clock;

struct Config {
    std::string mode{"matching"};
    std::string output{"benchmarks/raw/atlas-bench.csv"};
    std::size_t operations{10000};
    std::uint64_t seed{7245981};
};

struct Sample { std::string operation; std::uint64_t latency_ns{}; bool success{}; std::size_t trades{}; };

void usage(const char* name) {
    std::cout << "Usage: " << name << " --mode matching|wal|raft|snapshot|mixed --orders N --seed N --output CSV\n";
}

Config parse(int argc, char** argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") { usage(argv[0]); std::exit(0); }
        if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
        const std::string value = argv[++i];
        if (arg == "--mode") config.mode = value;
        else if (arg == "--orders" || arg == "--operations") config.operations = std::stoull(value);
        else if (arg == "--seed") config.seed = std::stoull(value);
        else if (arg == "--output") config.output = value;
        else throw std::invalid_argument("unknown option: " + arg);
    }
    if (!config.operations) throw std::invalid_argument("operation count must be positive");
    if (config.mode != "matching" && config.mode != "wal" && config.mode != "raft" &&
        config.mode != "snapshot" && config.mode != "mixed")
        throw std::invalid_argument("unsupported in-process benchmark mode");
    return config;
}

std::uint64_t mix64(std::uint64_t x) {
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

OrderRequest build_order(std::size_t i, std::uint64_t seed, bool mixed = false) {
    const auto bits = mix64(seed + static_cast<std::uint64_t>(i) * 0x9e3779b97f4a7c15ULL);
    const Side side = (i % 2 == 0) ? Side::Buy : Side::Sell;
    const PriceTicks price = mixed
        ? (side == Side::Buy ? 70 + static_cast<PriceTicks>((bits >> 16) % 20)
                             : 110 + static_cast<PriceTicks>((bits >> 16) % 20))
        : 90 + static_cast<PriceTicks>((bits >> 16) % 21);
    const auto id = static_cast<OrderId>(i + 1);
    return {id, static_cast<ClientId>(i % 128 + 1), id, 1, side, OrderType::Limit,
            price, static_cast<Quantity>((bits >> 8) % 5 + 1)};
}

std::uint64_t timed(const std::function<void()>& fn) {
    const auto start = Clock::now(); fn();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
}

class MemoryStorage final : public raft::PersistentStorage {
public:
    raft::PersistentState load() override { return state; }
    void save(const raft::PersistentState& next) override { state = next; }
    std::vector<raft::LogEntry> load_log() override { return log; }
    void save_log(const std::vector<raft::LogEntry>& next) override { log = next; }
    raft::LogIndex load_commit_index() override { return committed; }
    void save_commit_index(raft::LogIndex next) override { committed = next; }
    std::vector<raft::RequestOutcome> load_request_outcomes() override { return {}; }
    void save_request_outcome(const raft::RequestOutcome&) override {}
    raft::PersistentState state;
    std::vector<raft::LogEntry> log;
    raft::LogIndex committed{};
};

class FakeClock final : public raft::Clock {
public:
    raft::Milliseconds now_ms() const noexcept override { return now; }
    void advance(raft::Milliseconds value) { now += value; }
    raft::Milliseconds now{};
};
class FixedTimeout final : public raft::ElectionTimeoutSource {
public:
    raft::Milliseconds next_timeout_ms() override { return 1000; }
};
class Applied final : public raft::StateMachine {
public:
    void apply(raft::LogIndex index, const raft::LogEntry&) override { last = index; }
    raft::LogIndex last{};
};

std::vector<Sample> run_raft(std::size_t count, std::uint64_t seed) {
    FakeClock clock; FixedTimeout timeout;
    std::array<MemoryStorage, 3> stores;
    std::array<Applied, 3> machines;
    std::array<std::unique_ptr<raft::RaftNode>, 3> nodes;
    for (std::size_t i = 0; i < 3; ++i) {
        std::vector<raft::NodeId> peers;
        for (std::uint64_t id = 1; id <= 3; ++id) if (id != i + 1) peers.push_back(id);
        nodes[i] = std::make_unique<raft::RaftNode>(i + 1, peers, raft::LogMetadata{}, stores[i],
                                                     clock, timeout, &machines[i], 20);
    }
    clock.advance(1000); nodes[0]->poll();
    auto deliver = [&](std::size_t max_rounds = 100) {
        for (std::size_t round = 0; round < max_rounds; ++round) {
            bool had = false;
            for (std::size_t source = 0; source < 3; ++source) {
                auto outbound = nodes[source]->drain_outbox();
                if (!outbound.empty()) had = true;
                for (const auto& item : outbound) {
                    const auto target = static_cast<std::size_t>(item.target - 1);
                    (void)nodes[target]->receive_applied(source + 1, item.message);
                }
            }
            if (!had) break;
        }
    };
    deliver();
    if (nodes[0]->role() != raft::Role::Leader) throw std::runtime_error("controlled Raft leader election failed");
    std::vector<Sample> samples;
    samples.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        const auto request = build_order(i, seed);
        const auto command = protocol::encode(protocol::SubmitOrder{request.client_id, request.request_id,
            request.id, request.instrument_id, request.side == Side::Buy ? protocol::Side::Buy : protocol::Side::Sell,
            protocol::OrderType::Limit, request.limit_price, request.quantity});
        const auto start = Clock::now();
        const auto index = nodes[0]->append_command(command);
        deliver();
        if (nodes[0]->commit_index() < index || nodes[0]->last_applied() < index)
            throw std::runtime_error("controlled Raft command did not commit/apply");
        samples.push_back({"append_commit_apply", static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count()), true, 0});
        const auto committed_payload = nodes[0]->log()[index - 1].command;
        for (std::size_t replica = 1; replica < 3; ++replica) {
            if (nodes[replica]->commit_index() < index || nodes[replica]->last_applied() < index ||
                nodes[replica]->log().size() < index || nodes[replica]->log()[index - 1].command != committed_payload)
                throw std::runtime_error("controlled Raft replicas diverged after command commitment");
        }
    }
    return samples;
}

void write_samples(const Config& config, const std::vector<Sample>& samples, double elapsed,
                   double cpu_user, double cpu_system, long peak_rss_kib,
                   std::size_t rejected = 0, std::size_t errors = 0) {
    const std::filesystem::path path(config.output);
    if (!path.parent_path().empty()) std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    if (!out) throw std::runtime_error("cannot open raw CSV output");
    out << "mode,operation_index,operation,latency_ns,success,trades,seed,elapsed_s,accepted,rejected,errors,compiler,compiler_version,build_type,vm_configuration,client_count,node_count,workload_distribution,durability,trial,cpu_user_s,cpu_system_s,peak_rss_kib\n";
    const auto transaction_samples = std::count_if(samples.begin(), samples.end(), [](const auto& s) {
        return s.operation == "submit" || s.operation == "matching_submit" || s.operation == "cancel";
    });
    const auto accepted_transactions = transaction_samples == 0
        ? std::count_if(samples.begin(), samples.end(), [](const auto& s) { return s.success; })
        : std::count_if(samples.begin(), samples.end(), [](const auto& s) {
            return (s.operation == "submit" || s.operation == "matching_submit" || s.operation == "cancel") && s.success;
        });
    for (std::size_t i = 0; i < samples.size(); ++i) {
        const auto& s = samples[i];
        out << config.mode << ',' << i << ',' << s.operation << ',' << s.latency_ns << ',' << (s.success ? 1 : 0)
            << ',' << s.trades << ',' << config.seed << ',' << elapsed << ',' << accepted_transactions << ',' << rejected
            << ',' << errors << ',' << ATLAS_COMPILER_ID << ',' << ATLAS_COMPILER_VERSION << ',' << ATLAS_BUILD_MODE
            << ",unspecified,1,1,seeded deterministic workload," << config.mode << ",1,"
            << cpu_user << ',' << cpu_system << ',' << peak_rss_kib << '\n';
    }
}

} // namespace

int main(int argc, char** argv) {
    try {
        const auto config = parse(argc, argv);
        rusage usage_start{};
        if (::getrusage(RUSAGE_SELF, &usage_start) != 0) throw std::runtime_error("getrusage start failed");
        std::vector<Sample> samples;
        std::size_t rejected = 0, errors = 0;
        const auto total_start = Clock::now();

        if (config.mode == "matching" || config.mode == "mixed" || config.mode == "wal" || config.mode == "snapshot") {
            OrderBook book;
            std::filesystem::path temp;
            std::unique_ptr<WriteAheadLog> wal;
            if (config.mode == "wal" || config.mode == "snapshot") {
                temp = std::filesystem::temp_directory_path() /
                    ("atlas_bench_" + std::to_string(::getpid()) + "_" + std::to_string(config.seed));
                std::filesystem::remove_all(temp);
                wal = std::make_unique<WriteAheadLog>(temp / "commands.wal", WalDurabilityMode::Durable);
            }
            std::vector<OrderId> live;
            for (std::size_t i = 0; i < config.operations; ++i) {
                auto request = build_order(i, config.seed, config.mode == "mixed");
                if (config.mode == "mixed" && i % 4 == 3 && !live.empty()) {
                    const auto id = live.back(); live.pop_back();
                    bool canceled = false;
                    const auto ns = timed([&] {
                        const auto result = book.cancel(id);
                        canceled = result.canceled();
                        if (canceled && wal) (void)wal->append_cancel(id, result.command_sequence);
                    });
                    if (!canceled) ++rejected;
                    samples.push_back({"cancel", ns, canceled, 0});
                    continue;
                }
                const auto start = Clock::now();
                const auto result = book.submit(request);
                const auto match_finished = Clock::now();
                const auto matching_ns = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(match_finished - start).count());
                if (result.accepted()) {
                    if (wal) {
                        samples.push_back({"matching_submit", matching_ns, true, result.trades.size()});
                        const auto append_ns = timed([&] { (void)wal->append_submit(request, result.order->command_sequence); });
                        samples.push_back({"wal_append_fsync", append_ns, true, 0});
                    } else {
                        samples.push_back({"submit", matching_ns, true, result.trades.size()});
                    }
                } else {
                    ++rejected;
                    samples.push_back({"submit", matching_ns, false, 0});
                }
                if (result.accepted()) live.push_back(request.id);
            }
            if (config.mode == "snapshot") {
                const auto snap = temp / "book.snap";
                samples.push_back({"snapshot_create", timed([&] { (void)SnapshotStore::save(snap, *wal, book); }), true, 0});
                OrderBook recovered;
                samples.push_back({"snapshot_recover", timed([&] { (void)SnapshotStore::recover(snap, temp / "commands.wal", recovered); }), true, 0});
                const auto expected = book.export_state();
                const auto actual = recovered.export_state();
                const auto same_order = [](const OrderState& lhs, const OrderState& rhs) {
                    return lhs.request.id == rhs.request.id && lhs.request.client_id == rhs.request.client_id &&
                        lhs.request.request_id == rhs.request.request_id && lhs.request.instrument_id == rhs.request.instrument_id &&
                        lhs.request.side == rhs.request.side && lhs.request.type == rhs.request.type &&
                        lhs.request.limit_price == rhs.request.limit_price && lhs.request.quantity == rhs.request.quantity &&
                        lhs.command_sequence == rhs.command_sequence && lhs.filled_quantity == rhs.filled_quantity &&
                        lhs.leaves_quantity == rhs.leaves_quantity && lhs.canceled_quantity == rhs.canceled_quantity &&
                        lhs.status == rhs.status;
                };
                bool orders_match = expected.orders.size() == actual.orders.size();
                for (std::size_t order_index = 0; orders_match && order_index < expected.orders.size(); ++order_index)
                    orders_match = same_order(expected.orders[order_index], actual.orders[order_index]);
                if (!recovered.invariants_hold() || !orders_match || expected.instrument_id != actual.instrument_id ||
                    expected.next_command_sequence != actual.next_command_sequence ||
                    expected.next_trade_sequence != actual.next_trade_sequence)
                    throw std::runtime_error("snapshot recovery state mismatch");
            }
            if (!temp.empty()) std::filesystem::remove_all(temp);
        } else if (config.mode == "raft") {
            samples = run_raft(config.operations, config.seed);
        }
        const double elapsed = std::chrono::duration<double>(Clock::now() - total_start).count();
        rusage usage_end{};
        if (::getrusage(RUSAGE_SELF, &usage_end) != 0) throw std::runtime_error("getrusage end failed");
        const auto time_seconds = [](const timeval& value) {
            return static_cast<double>(value.tv_sec) + static_cast<double>(value.tv_usec) / 1'000'000.0;
        };
        const double cpu_user = time_seconds(usage_end.ru_utime) - time_seconds(usage_start.ru_utime);
        const double cpu_system = time_seconds(usage_end.ru_stime) - time_seconds(usage_start.ru_stime);
        write_samples(config, samples, elapsed, cpu_user, cpu_system, usage_end.ru_maxrss, rejected, errors);
        std::cout << "mode=" << config.mode << " samples=" << samples.size() << " elapsed_s=" << elapsed
                  << " seed=" << config.seed << " compiler=" << ATLAS_COMPILER_ID << "-" << ATLAS_COMPILER_VERSION
                  << " build_type=" << ATLAS_BUILD_MODE << " raw_csv=" << config.output << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas-bench: " << error.what() << '\n'; usage(argv[0]); return 1;
    }
}
