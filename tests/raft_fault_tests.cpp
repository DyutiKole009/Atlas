#include "atlas/protocol.hpp"
#include "atlas/raft.hpp"
#include "atlas/raft_storage.hpp"
#include "atlas/replicated_exchange.hpp"
#include "atlas/storage.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <chrono>
#include <sys/wait.h>
#include <unistd.h>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {
using namespace atlas;
using namespace atlas::raft;

constexpr std::uint64_t kReproducibleSeed = 0xA71A5FA017ULL;
constexpr std::size_t kMaxTicks = 1200;
constexpr std::size_t kMaxDeliveriesPerTick = 40;

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

class FakeClock final : public Clock {
public:
    Milliseconds now_ms() const noexcept override { return now_; }
    void advance(Milliseconds delta) { now_ += delta; }
private:
    Milliseconds now_{};
};

class FixedTimeout final : public ElectionTimeoutSource {
public:
    explicit FixedTimeout(Milliseconds timeout) : timeout_(timeout) {}
    Milliseconds next_timeout_ms() override { return timeout_; }
private:
    Milliseconds timeout_;
};

class MemoryStorage : public PersistentStorage {
public:
    PersistentState load() override { return state; }
    void save(const PersistentState& value) override { state = value; }
    std::vector<LogEntry> load_log() override { return entries; }
    void save_log(const std::vector<LogEntry>& value) override { entries = value; }
    LogIndex load_commit_index() override { return committed; }
    void save_commit_index(LogIndex value) override { committed = value; }
    std::vector<RequestOutcome> load_request_outcomes() override { return outcomes; }
    void save_request_outcome(const RequestOutcome& outcome) override {
        for (const auto& previous : outcomes) {
            if (previous.client_id == outcome.client_id && previous.request_id == outcome.request_id) {
                require(previous == outcome, "request outcome key remains immutable");
                return;
            }
        }
        outcomes.push_back(outcome);
    }
    PersistentState state;
    std::vector<LogEntry> entries;
    LogIndex committed{};
    std::vector<RequestOutcome> outcomes;
};

std::uint64_t next_random(std::uint64_t& state) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

struct NodeFixture {
    explicit NodeFixture(NodeId id, FakeClock& clock, MemoryStorage& storage)
                : id(id), storage(storage), state_machine(std::make_unique<ReplicatedExchangeStateMachine>(storage)), timeout(71 + id * 17),
          node(std::make_unique<RaftNode>(id, peers(id), LogMetadata{}, storage, clock, timeout,
                                                                                    state_machine.get(), 9)) {}

    static std::vector<NodeId> peers(NodeId id) {
        std::vector<NodeId> result;
        for (NodeId other = 1; other <= 3; ++other) if (other != id) result.push_back(other);
        return result;
    }

    NodeId id;
    MemoryStorage& storage;
    std::unique_ptr<ReplicatedExchangeStateMachine> state_machine;
    FixedTimeout timeout;
    std::unique_ptr<RaftNode> node;
};

struct Envelope { NodeId from; NodeId to; Message message; };

class FaultNetwork {
public:
    explicit FaultNetwork(std::uint64_t seed) : random_(seed) {}

    void add(NodeFixture& fixture) { nodes_[fixture.id] = &fixture; }
    void isolate(std::optional<NodeId> node) { isolated_ = node; }

    void tick(FakeClock& clock) {
        clock.advance(5);
        for (auto& [id, fixture] : nodes_) {
            (void)id;
            fixture->node->poll();
        }
        collect();
        for (std::size_t delivery = 0; delivery < kMaxDeliveriesPerTick && !queue_.empty(); ++delivery) {
            const std::size_t selected = static_cast<std::size_t>(next_random(random_) % queue_.size());
            Envelope envelope = std::move(queue_[selected]);
            queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(selected));
            if (selected != 0) ++reordered_;
            const bool partitioned = isolated_ &&
                (envelope.from == *isolated_ || envelope.to == *isolated_);
            if (partitioned) {
                ++partition_drops_;
                continue;
            }
            // Reproducible fault schedule: drop 1/13, delay 1/5 by requeueing,
            // duplicate 1/11 once. The bounded tick count prevents infinite retries.
            const auto fault = next_random(random_) % 143;
            if (fault % 13 == 0) {
                ++dropped_;
                continue;
            }
            if (fault % 5 == 0) {
                queue_.push_back(std::move(envelope));
                ++delayed_;
                continue;
            }
            const bool duplicate = fault % 11 == 0;
            deliver(envelope);
            if (duplicate) {
                queue_.push_back(std::move(envelope));
                ++duplicated_;
            }
            collect();
        }
    }

    [[nodiscard]] std::size_t dropped() const noexcept { return dropped_; }
    [[nodiscard]] std::size_t delayed() const noexcept { return delayed_; }
    [[nodiscard]] std::size_t duplicated() const noexcept { return duplicated_; }
    [[nodiscard]] std::size_t partition_drops() const noexcept { return partition_drops_; }
    [[nodiscard]] std::size_t reordered() const noexcept { return reordered_; }

private:
    void collect() {
        for (auto& [id, fixture] : nodes_) {
            for (auto& outbound : fixture->node->drain_outbox())
                queue_.push_back({id, outbound.target, std::move(outbound.message)});
        }
    }

    void deliver(const Envelope& envelope) {
        const auto target = nodes_.find(envelope.to);
        if (target != nodes_.end()) target->second->node->receive(envelope.from, envelope.message);
    }

    std::map<NodeId, NodeFixture*> nodes_;
    std::deque<Envelope> queue_;
    std::uint64_t random_;
    std::optional<NodeId> isolated_;
    std::size_t dropped_{};
    std::size_t delayed_{};
    std::size_t duplicated_{};
    std::size_t partition_drops_{};
    std::size_t reordered_{};
};

std::vector<std::unique_ptr<NodeFixture>> make_cluster(FakeClock& clock,
                                                       std::array<MemoryStorage, 3>& stores) {
    std::vector<std::unique_ptr<NodeFixture>> result;
    for (NodeId id = 1; id <= 3; ++id)
        result.push_back(std::make_unique<NodeFixture>(id, clock, stores[id - 1]));
    return result;
}

NodeFixture* leader_of(const std::vector<std::unique_ptr<NodeFixture>>& nodes) {
    NodeFixture* found = nullptr;
    for (const auto& fixture : nodes) {
        if (fixture->node->role() != Role::Leader) continue;
        if (found) return nullptr;
        found = fixture.get();
    }
    return found;
}

bool await_leader(FaultNetwork& network, FakeClock& clock,
                  const std::vector<std::unique_ptr<NodeFixture>>& nodes,
                  std::size_t max_ticks = 500) {
    for (std::size_t tick = 0; tick < max_ticks; ++tick) {
        if (leader_of(nodes)) return true;
        network.tick(clock);
    }
    return leader_of(nodes) != nullptr;
}

protocol::Message submit_command(std::uint64_t client, std::uint64_t request, std::uint64_t order,
                                 protocol::Side side, std::int64_t price, std::uint64_t quantity) {
    return protocol::SubmitOrder{client, request, order, 1, side, protocol::OrderType::Limit,
                                 price, quantity};
}

bool all_committed_applied(const std::vector<std::unique_ptr<NodeFixture>>& nodes,
                           LogIndex index) {
    return std::all_of(nodes.begin(), nodes.end(), [index](const auto& fixture) {
        return fixture->node->commit_index() >= index && fixture->node->last_applied() >= index;
    });
}

bool quorum_committed_applied(const std::vector<std::unique_ptr<NodeFixture>>& nodes,
                              LogIndex index) {
    const auto ready = std::count_if(nodes.begin(), nodes.end(), [index](const auto& fixture) {
        return fixture->node->commit_index() >= index && fixture->node->last_applied() >= index;
    });
    return ready >= 2;
}

void assert_committed_prefix_safety(const std::vector<std::unique_ptr<NodeFixture>>& nodes) {
    for (std::size_t left = 0; left < nodes.size(); ++left) {
        for (std::size_t right = left + 1; right < nodes.size(); ++right) {
            const auto committed = std::min(nodes[left]->node->commit_index(),
                                            nodes[right]->node->commit_index());
            for (LogIndex index = 1; index <= committed; ++index) {
                require(nodes[left]->node->log()[index - 1] == nodes[right]->node->log()[index - 1],
                        "nodes never disagree on a mutually committed log entry");
            }
                require(nodes[left]->state_machine->book().invariants_hold() &&
                    nodes[right]->state_machine->book().invariants_hold(),
                    "matching state remains internally valid");
        }
    }
}

void test_seeded_faults_partition_leader_restart_and_dedup() {
    FakeClock clock;
    std::array<MemoryStorage, 3> stores;
    auto nodes = make_cluster(clock, stores);
    FaultNetwork network(kReproducibleSeed);
    for (auto& fixture : nodes) network.add(*fixture);

    require(await_leader(network, clock, nodes), "bounded seeded delivery elects an initial leader");
    auto* initial_leader = leader_of(nodes);
    require(initial_leader != nullptr, "exactly one initial leader exists");

    const auto first = submit_command(10, 1, 100, protocol::Side::Sell, 105, 5);
    const auto first_bytes = protocol::encode(first);
    const LogIndex first_index = initial_leader->node->append_command(first_bytes);
    bool committed = false;
    for (std::size_t tick = 0; tick < 500 && !committed; ++tick) {
        network.tick(clock);
        assert_committed_prefix_safety(nodes);
        committed = initial_leader->node->commit_index() >= first_index &&
                    initial_leader->node->last_applied() >= first_index;
    }
    require(committed && quorum_committed_applied(nodes, first_index),
            "the success condition is only true after leader and quorum commit/application");
    for (std::size_t tick = 0; tick < 300 && !all_committed_applied(nodes, first_index); ++tick)
        network.tick(clock);
    require(all_committed_applied(nodes, first_index), "all healthy peers apply first command");
    assert_committed_prefix_safety(nodes);

    const auto second = submit_command(11, 1, 101, protocol::Side::Buy, 105, 2);
    const auto second_bytes = protocol::encode(second);
    const LogIndex second_index = initial_leader->node->append_command(second_bytes);
    for (std::size_t tick = 0; tick < 500 && !quorum_committed_applied(nodes, second_index); ++tick) {
        network.tick(clock);
        assert_committed_prefix_safety(nodes);
    }
    require(quorum_committed_applied(nodes, second_index), "matching command commits and applies on a quorum despite seeded faults");
    for (std::size_t tick = 0; tick < 300 && !all_committed_applied(nodes, second_index); ++tick)
        network.tick(clock);
    require(all_committed_applied(nodes, second_index), "all nodes catch up and apply the second command");

    // Isolate the original leader, then create an uncommitted suffix there.
    const NodeId old_leader_id = initial_leader->id;
    network.isolate(old_leader_id);
    const auto uncertain = submit_command(12, 1, 102, protocol::Side::Buy, 90, 1);
    (void)initial_leader->node->append_command(protocol::encode(uncertain));
    const auto committed_before_isolation = initial_leader->node->commit_index();
    for (std::size_t tick = 0; tick < 30; ++tick) network.tick(clock);
    require(initial_leader->node->commit_index() == committed_before_isolation &&
            initial_leader->node->last_applied() == committed_before_isolation,
            "isolated former leader cannot commit or apply without a quorum; temporary unavailability is expected");

    NodeFixture* replacement = nullptr;
    for (std::size_t tick = 0; tick < 500 && !replacement; ++tick) {
        network.tick(clock);
        for (const auto& fixture : nodes)
            if (fixture->id != old_leader_id && fixture->node->role() == Role::Leader)
                replacement = fixture.get();
    }
    require(replacement != nullptr && replacement->node->current_term() > initial_leader->node->current_term(),
            "the remaining quorum elects a higher-term leader");
    const auto replacement_command = submit_command(13, 1, 103, protocol::Side::Buy, 95, 1);
    const auto replacement_index = replacement->node->append_command(protocol::encode(replacement_command));
    for (std::size_t tick = 0; tick < 500 && !quorum_committed_applied(nodes, replacement_index); ++tick) {
        network.tick(clock);
        assert_committed_prefix_safety(nodes);
    }
    require(replacement->node->commit_index() >= replacement_index &&
            replacement->node->last_applied() >= replacement_index &&
            quorum_committed_applied(nodes, replacement_index),
            "replacement leader commits and applies with its connected majority");

    // Restart the old leader from persisted term, vote and divergent uncommitted log, then heal.
    auto& old_fixture = *std::find_if(nodes.begin(), nodes.end(), [old_leader_id](const auto& fixture) {
        return fixture->id == old_leader_id;
    })->get();
    old_fixture.node.reset();
    old_fixture.state_machine = std::make_unique<ReplicatedExchangeStateMachine>(old_fixture.storage);
    old_fixture.node = std::make_unique<RaftNode>(old_fixture.id, NodeFixture::peers(old_fixture.id),
        LogMetadata{old_fixture.storage.entries.empty() ? 0 : old_fixture.storage.entries.back().term,
                    old_fixture.storage.entries.size()}, old_fixture.storage, clock, old_fixture.timeout,
        old_fixture.state_machine.get(), 9);
    require(old_fixture.node->role() == Role::Follower,
            "restarted former leader starts as follower and cannot claim leadership from stale state");
    network.isolate(std::nullopt);

    for (std::size_t tick = 0; tick < 700 && !all_committed_applied(nodes, replacement_index); ++tick) {
        network.tick(clock);
        assert_committed_prefix_safety(nodes);
    }
    require(all_committed_applied(nodes, replacement_index), "restarted node catches up through the committed prefix");
    require(old_fixture.node->current_term() >= replacement->node->current_term(),
            "restarted former leader observes the valid current term");
    require(old_fixture.node->log() == replacement->node->log(),
            "divergent uncommitted suffix is repaired to the elected leader's log");
        require(old_fixture.state_machine->book().export_state().orders.size() ==
            replacement->state_machine->book().export_state().orders.size(),
            "replicas reconstruct the same matching order count");
        require(old_fixture.state_machine->book().export_state().next_command_sequence ==
            replacement->state_machine->book().export_state().next_command_sequence,
            "replicas reconstruct matching command sequence state");
    assert_committed_prefix_safety(nodes);

    const auto dedup_result = old_fixture.state_machine->result(11, 1);
    require(dedup_result.has_value() && dedup_result->messages.size() == 1 &&
            std::holds_alternative<protocol::OrderResponse>(dedup_result->messages.front()),
            "recovered exact request retry exposes its durable terminal response without trade replay");
    auto conflict = submit_command(11, 1, 101, protocol::Side::Buy, 106, 2);
    require(protocol::encode(conflict) != second_bytes,
            "conflicting retry payload is distinguishable by its canonical fingerprint");

        require(network.dropped() > 0 && network.delayed() > 0 && network.duplicated() > 0 &&
            network.reordered() > 0 &&
            network.partition_drops() > 0,
            "seeded run exercised drops, delays, duplicates, reordering, and isolation");
}

void test_log_storage_failure_and_snapshot_boundary() {
    class FailingLogStore final : public MemoryStorage {
    public:
        void save_log(const std::vector<LogEntry>& value) override {
            if (fail_next) { fail_next = false; throw RaftError("injected crash before durable log publication"); }
            MemoryStorage::save_log(value);
        }
        bool fail_next{false};
    } storage;
    FakeClock clock;
    FixedTimeout timeout(100);
    RaftNode follower(2, {1, 3}, {}, storage, clock, timeout);
    storage.fail_next = true;
    bool failed = false;
    try {
        follower.receive(1, AppendEntriesRequest{1, 1, 0, 0, {{1, {0x7}}}, 0});
    } catch (const RaftError&) { failed = true; }
    require(failed && follower.log().empty() && follower.drain_outbox().empty(),
            "injected durable-log failure cannot acknowledge an unpersisted command");

    // Raft snapshots and log compaction are not implemented in this project. This is an explicit
    // unsupported boundary, not a passing snapshot-installation or compaction guarantee.
    require(follower.commit_index() == 0 && follower.last_applied() == 0,
            "without snapshot installation the follower remains at its unchanged applied prefix");
}

void test_process_crash_during_atomic_publication() {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto root = std::filesystem::temp_directory_path() /
        ("atlas_fault_publication_" + std::to_string(::getpid()) + "_" + std::to_string(stamp));
    std::filesystem::create_directories(root);
    try {
        const auto raft_directory = root / "raft";
        {
            FilePersistentStorage initial(raft_directory);
            initial.save(PersistentState{1, std::nullopt});
            initial.save_log({LogEntry{1, {0x01}}});
        }
        const pid_t raft_child = ::fork();
        require(raft_child >= 0, "fork Raft atomic-publication crash child");
        if (raft_child == 0) {
            try {
                FilePersistentStorage storage(raft_directory, [](FileStorePublicationStage stage) {
                    if (stage == FileStorePublicationStage::BytesWritten) ::_exit(71);
                });
                storage.save_log({LogEntry{1, {0x01}}, LogEntry{1, {0x02}}});
            } catch (...) { ::_exit(72); }
            ::_exit(73);
        }
        int status = 0;
        require(::waitpid(raft_child, &status, 0) == raft_child && WIFEXITED(status) &&
                WEXITSTATUS(status) == 71, "Raft child stops after temp write and before publication");
        {
            FilePersistentStorage recovered(raft_directory);
            require(recovered.load_log() == std::vector<LogEntry>{LogEntry{1, {0x01}}},
                    "crash during unpublished Raft temp write leaves the prior complete log intact");
        }

        const auto wal_path = root / "book.wal";
        const auto snapshot_path = root / "book.snap";
        OrderBook source;
        const OrderRequest request{7, 7, 7, 1, Side::Buy, OrderType::Limit, 90, 2};
        const auto accepted = source.submit(request);
        require(accepted.accepted() && accepted.order.has_value(), "snapshot fixture order accepted");
        WriteAheadLog wal(wal_path, WalDurabilityMode::Durable);
        require(wal.append_submit(request, accepted.order->command_sequence).acknowledged,
                "snapshot fixture command durably logged");

        const pid_t snapshot_child = ::fork();
        require(snapshot_child >= 0, "fork snapshot publication crash child");
        if (snapshot_child == 0) {
            (void)SnapshotStore::save(snapshot_path, wal, source, [](SnapshotPublicationStage stage) {
                if (stage == SnapshotPublicationStage::BytesWritten) ::_exit(81);
            });
            ::_exit(82);
        }
        status = 0;
        require(::waitpid(snapshot_child, &status, 0) == snapshot_child && WIFEXITED(status) &&
                WEXITSTATUS(status) == 81, "snapshot child stops after temp bytes are written");
        OrderBook recovered_book;
        const auto replay = SnapshotStore::recover(snapshot_path, wal_path, recovered_book);
        require(replay.accepted_commands == 1 && recovered_book.invariants_hold() &&
                recovered_book.export_state().orders.size() == 1,
                "crash before snapshot rename safely recovers from the durable WAL");
    } catch (...) {
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
        test_seeded_faults_partition_leader_restart_and_dedup();
        test_log_storage_failure_and_snapshot_boundary();
        test_process_crash_during_atomic_publication();
        std::cout << "atlas_raft_fault_tests: passed; seed=0x" << std::hex << kReproducibleSeed
                  << std::dec << "; bounded ticks=" << kMaxTicks
                  << "; snapshot install/log compaction are unsupported and not tested\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas_raft_fault_tests: seed=0x" << std::hex << kReproducibleSeed
                  << std::dec << ": " << error.what() << '\n';
        return 1;
    }
}
