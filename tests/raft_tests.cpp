#include "atlas/raft.hpp"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace {
using namespace atlas::raft;

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

class FakeClock final : public Clock {
public:
    Milliseconds now_ms() const noexcept override { return now_; }
    void advance_to(Milliseconds value) {
        if (value < now_) throw std::runtime_error("fake clock cannot move backward");
        now_ = value;
    }
private:
    Milliseconds now_{0};
};

class FixedTimeout final : public ElectionTimeoutSource {
public:
    explicit FixedTimeout(Milliseconds timeout) : timeout_(timeout) {}
    Milliseconds next_timeout_ms() override { return timeout_; }
private:
    Milliseconds timeout_;
};

class MemoryPersistentStorage final : public PersistentStorage {
public:
    PersistentState load() override { return state; }
    void save(const PersistentState& next) override {
        state = next;
        ++save_count;
    }
    std::vector<LogEntry> load_log() override { return entries; }
    void save_log(const std::vector<LogEntry>& next) override {
        entries = next;
        ++log_save_count;
    }
    LogIndex load_commit_index() override { return committed; }
    void save_commit_index(LogIndex next) override { committed = next; }
    std::vector<RequestOutcome> load_request_outcomes() override { return outcomes; }
    void save_request_outcome(const RequestOutcome& outcome) override {
        outcomes.push_back(outcome);
    }
    PersistentState state{};
    std::vector<LogEntry> entries;
    std::size_t save_count{0};
    std::size_t log_save_count{0};
    LogIndex committed{0};
    std::vector<RequestOutcome> outcomes;
};

class RecordingStateMachine final : public StateMachine {
public:
    void apply(LogIndex index, const LogEntry& entry) override {
        applied.emplace_back(index, entry);
    }
    std::vector<std::pair<LogIndex, LogEntry>> applied;
};

class SimulatedNetwork {
public:
    struct Envelope { NodeId from; NodeId to; Message message; };

    void add(RaftNode& node) { nodes_.emplace(node.id(), &node); }
    void poll_all() {
        for (const auto& [id, node] : nodes_) {
            (void)id;
            node->poll();
        }
        collect_outboxes();
    }
    void collect_outboxes() {
        for (const auto& [id, node] : nodes_) {
            for (auto& outbound : node->drain_outbox()) {
                queue_.push_back(Envelope{id, outbound.target, std::move(outbound.message)});
            }
        }
    }
    bool deliver_next() {
        if (queue_.empty()) return false;
        Envelope envelope = std::move(queue_.front());
        queue_.pop_front();
        const auto target = nodes_.find(envelope.to);
        if (target != nodes_.end()) target->second->receive(envelope.from, envelope.message);
        collect_outboxes();
        return true;
    }
    bool deliver_at(std::size_t index) {
        if (index >= queue_.size()) return false;
        Envelope envelope = std::move(queue_[index]);
        queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(index));
        const auto target = nodes_.find(envelope.to);
        if (target != nodes_.end()) target->second->receive(envelope.from, envelope.message);
        collect_outboxes();
        return true;
    }
    void deliver_all() {
        std::size_t safety = 10000;
        while (!queue_.empty() && safety-- != 0) deliver_next();
        require(safety != 0, "simulated network message loop terminates");
    }
    void drop_all() { queue_.clear(); }
    [[nodiscard]] std::size_t queued() const noexcept { return queue_.size(); }
private:
    std::unordered_map<NodeId, RaftNode*> nodes_;
    std::deque<Envelope> queue_;
};

void test_competing_candidates_split_votes() {
    FakeClock clock;
    FixedTimeout timeout(10);
    MemoryPersistentStorage s1, s2, s3;
    RaftNode n1(1, {2, 3}, {}, s1, clock, timeout);
    RaftNode n2(2, {1, 3}, {}, s2, clock, timeout);
    RaftNode n3(3, {1, 2}, {}, s3, clock, timeout);
    SimulatedNetwork network;
    network.add(n1); network.add(n2); network.add(n3);
    clock.advance_to(10);
    network.poll_all();
    require(n1.role() == Role::Candidate && n2.role() == Role::Candidate && n3.role() == Role::Candidate,
            "simultaneous timeouts start competing candidacies");
    network.deliver_all();
    require(n1.current_term() == 1 && n2.current_term() == 1 && n3.current_term() == 1,
            "candidates remain in same term");
    require(n1.role() != Role::Leader && n2.role() != Role::Leader && n3.role() != Role::Leader,
            "no candidate gets more than one vote in the split election");
    require(s1.state.voted_for == 1 && s2.state.voted_for == 2 && s3.state.voted_for == 3,
            "self votes are persisted");
}

void test_single_candidate_wins_majority_and_votes_persist_before_send() {
    FakeClock clock;
    FixedTimeout timeout(15);
    MemoryPersistentStorage s1, s2, s3;
    RaftNode n1(1, {2, 3}, {}, s1, clock, timeout);
    RaftNode n2(2, {1, 3}, {}, s2, clock, timeout);
    RaftNode n3(3, {1, 2}, {}, s3, clock, timeout);
    SimulatedNetwork network;
    network.add(n1); network.add(n2); network.add(n3);

    clock.advance_to(15);
    n1.poll();
    auto requests = n1.drain_outbox();
    require(n1.role() == Role::Candidate && n1.current_term() == 1 && n1.voted_for() == 1,
            "candidate state established");
    require(s1.state.current_term == 1 && s1.state.voted_for == 1 && s1.save_count == 1,
            "term and self-vote stored before vote requests are exposed");
    for (auto& request : requests) {
        const NodeId target = request.target;
        const auto* rpc = std::get_if<RequestVoteRequest>(&request.message);
        require(rpc != nullptr && rpc->term == 1 && rpc->candidate_id == 1,
                "outbound RequestVote contains candidate term and identity");
        (target == 2 ? n2 : n3).receive(1, request.message);
    }
    require(s2.state.current_term == 1 && s2.state.voted_for == 1,
            "follower persists granted vote");
    require(n2.drain_outbox().size() == 1, "follower emits vote response after persistence");
    n1.receive(2, RequestVoteResponse{1, true});
    require(n1.role() == Role::Leader, "self plus one of two peer votes reaches majority");
}

void test_stale_candidate_is_denied() {
    FakeClock clock;
    FixedTimeout timeout(100);
    MemoryPersistentStorage local_storage, candidate_storage;
    local_storage.state.current_term = 3;
    candidate_storage.state.current_term = 2;
    local_storage.entries = {{1, {}}, {2, {}}, {2, {}}, {3, {}}};
    candidate_storage.entries.resize(99, LogEntry{2, {}});
    RaftNode local(1, {2}, LogMetadata{3, 4}, local_storage, clock, timeout);
    RaftNode candidate(2, {1}, LogMetadata{2, 99}, candidate_storage, clock, timeout);
    local.receive(2, RequestVoteRequest{4, 2, 99, 2});
    require(local.current_term() == 4 && !local.voted_for().has_value(),
            "stale candidate's term may advance without granting vote");
    const auto messages = local.drain_outbox();
    require(messages.size() == 1, "stale candidate receives a response");
    const auto* response = std::get_if<RequestVoteResponse>(&messages[0].message);
    require(response && !response->vote_granted && response->term == 4,
            "candidate with older last-log term is denied despite longer log");
    (void)candidate;
}

void test_higher_term_steps_candidate_down() {
    FakeClock clock;
    FixedTimeout timeout(10);
    MemoryPersistentStorage s1, s2;
    RaftNode candidate(1, {2}, {}, s1, clock, timeout);
    RaftNode peer(2, {1}, {}, s2, clock, timeout);
    clock.advance_to(10);
    candidate.poll();
    require(candidate.role() == Role::Candidate, "node enters candidate role");
    candidate.receive(2, RequestVoteResponse{7, false});
    require(candidate.role() == Role::Follower && candidate.current_term() == 7 &&
                !candidate.voted_for().has_value(),
            "higher-term response forces step-down and clears vote");
    require(s1.state.current_term == 7 && !s1.state.voted_for.has_value(),
            "higher term persisted before transition completes");
    (void)peer;
}

void test_repeated_election_attempts_increment_term() {
    FakeClock clock;
    FixedTimeout timeout(5);
    MemoryPersistentStorage storage;
    RaftNode node(1, {2, 3}, {}, storage, clock, timeout);
    clock.advance_to(5);
    node.poll();
    require(node.current_term() == 1 && node.voted_for() == 1, "first election term");
    (void)node.drain_outbox();
    clock.advance_to(10);
    node.poll();
    require(node.current_term() == 2 && node.voted_for() == 1 && node.role() == Role::Candidate,
            "timeout without majority begins next term and votes for self again");
    require(storage.state.current_term == 2 && storage.state.voted_for == 1,
            "repeated term persisted");
}

void test_restart_recovers_vote_and_rejects_second_candidate() {
    FakeClock clock;
    FixedTimeout timeout(50);
    MemoryPersistentStorage storage;
    {
        RaftNode first(1, {2, 3}, {}, storage, clock, timeout);
        first.receive(2, RequestVoteRequest{4, 2, 0, 0});
        const auto response = first.drain_outbox();
        require(response.size() == 1 && std::get<RequestVoteResponse>(response[0].message).vote_granted,
                "first candidate receives vote");
        require(storage.state.current_term == 4 && storage.state.voted_for == 2,
                "durable vote is stored");
    }
    RaftNode restarted(1, {2, 3}, {}, storage, clock, timeout);
    require(restarted.role() == Role::Follower && restarted.current_term() == 4 &&
                restarted.voted_for() == 2,
            "restart recovers persistent term and vote as follower");
    restarted.receive(3, RequestVoteRequest{4, 3, 0, 0});
    const auto response = restarted.drain_outbox();
    require(response.size() == 1 && !std::get<RequestVoteResponse>(response[0].message).vote_granted,
            "restarted node does not cast a second vote in the same term");
}

void test_higher_term_request_and_log_index_tie_break() {
    FakeClock clock;
    FixedTimeout timeout(100);
    MemoryPersistentStorage storage;
    storage.state.current_term = 4;
    storage.entries.resize(8, LogEntry{4, {}});
    RaftNode node(1, {2, 3}, LogMetadata{4, 8}, storage, clock, timeout);
    node.receive(2, RequestVoteRequest{5, 2, 8, 4});
    require(node.current_term() == 5 && node.voted_for() == 2 && storage.state.voted_for == 2,
            "higher-term fresh candidate gets persisted vote");
    const auto grant = node.drain_outbox();
    require(grant.size() == 1 && std::get<RequestVoteResponse>(grant[0].message).vote_granted,
            "same last term and index is fresh enough");

    node.receive(3, RequestVoteRequest{5, 3, 7, 4});
    const auto denied = node.drain_outbox();
    require(denied.size() == 1 && !std::get<RequestVoteResponse>(denied[0].message).vote_granted,
            "shorter index at same last-log term is denied");
}

class FailingStorage final : public PersistentStorage {
public:
    PersistentState load() override { return state; }
    void save(const PersistentState&) override { throw RaftError("simulated durable-store failure"); }
    std::vector<LogEntry> load_log() override { return {}; }
    void save_log(const std::vector<LogEntry>&) override { throw RaftError("simulated durable-log failure"); }
    LogIndex load_commit_index() override { return 0; }
    void save_commit_index(LogIndex) override { throw RaftError("simulated commit-index failure"); }
    std::vector<RequestOutcome> load_request_outcomes() override { return {}; }
    void save_request_outcome(const RequestOutcome&) override { throw RaftError("simulated outcome failure"); }
    PersistentState state{};
};

class FailingLogStorage final : public PersistentStorage {
public:
    PersistentState load() override { return state; }
    void save(const PersistentState& next) override { state = next; }
    std::vector<LogEntry> load_log() override { return entries; }
    void save_log(const std::vector<LogEntry>&) override { throw RaftError("simulated durable-log failure"); }
    LogIndex load_commit_index() override { return 0; }
    void save_commit_index(LogIndex) override { throw RaftError("simulated commit-index failure"); }
    std::vector<RequestOutcome> load_request_outcomes() override { return {}; }
    void save_request_outcome(const RequestOutcome&) override { throw RaftError("simulated outcome failure"); }
    PersistentState state{};
    std::vector<LogEntry> entries;
};

void test_storage_failure_prevents_election_messages() {
    FakeClock clock;
    FixedTimeout timeout(5);
    FailingStorage storage;
    RaftNode node(1, {2, 3}, {}, storage, clock, timeout);
    clock.advance_to(5);
    bool failed = false;
    try { node.poll(); } catch (const RaftError&) { failed = true; }
    require(failed && node.current_term() == 0 && node.role() == Role::Follower,
            "failed durable term/vote write prevents candidacy");
    require(node.drain_outbox().empty(), "failed persistence emits no RequestVote messages");
}

void test_log_persistence_failure_prevents_replication_ack() {
    FakeClock clock;
    FixedTimeout timeout(100);
    FailingLogStorage storage;
    RaftNode follower(2, {1}, {}, storage, clock, timeout);
    bool failed = false;
    try {
        follower.receive(1, AppendEntriesRequest{1, 1, 0, 0, {{1, {0x55}}}, 0});
    } catch (const RaftError&) { failed = true; }
    require(failed && follower.current_term() == 1 && follower.log().empty(),
            "failed durable log replacement leaves no in-memory replicated entry");
    require(follower.drain_outbox().empty(), "failed log persistence emits no AppendEntries success");
}

void test_stale_leader_append_entries_rejected() {
    FakeClock clock;
    FixedTimeout timeout(100);
    MemoryPersistentStorage storage;
    storage.state.current_term = 5;
    RaftNode follower(2, {1}, {}, storage, clock, timeout);
    follower.receive(1, AppendEntriesRequest{4, 1, 0, 0, {}, 0});
    const auto replies = follower.drain_outbox();
    require(replies.size() == 1, "stale leader receives AppendEntries response");
    const auto* response = std::get_if<AppendEntriesResponse>(&replies.front().message);
    require(response && !response->success && response->term == 5,
            "stale-term leader cannot replicate or reset current term");
}

    void elect_two_node_leader(RaftNode& candidate, RaftNode& voter, FakeClock& clock,
                               NodeId candidate_id, NodeId voter_id, Milliseconds election_time,
                               bool discard_initial_heartbeat = true) {
        clock.advance_to(election_time);
        candidate.poll();
        auto requests = candidate.drain_outbox();
        const auto request = std::find_if(requests.begin(), requests.end(), [&](const OutboundMessage& item) {
        return item.target == voter_id && std::holds_alternative<RequestVoteRequest>(item.message);
        });
        require(request != requests.end(), "candidate sends RequestVote to voter");
        voter.receive(candidate_id, request->message);
        const auto responses = voter.drain_outbox();
        const auto response = std::find_if(responses.begin(), responses.end(), [&](const OutboundMessage& item) {
        return item.target == candidate_id && std::holds_alternative<RequestVoteResponse>(item.message);
        });
        require(response != responses.end(), "voter returns RequestVote response");
        candidate.receive(voter_id, response->message);
        require(candidate.role() == Role::Leader, "candidate wins two-node majority");
        if (discard_initial_heartbeat) (void)candidate.drain_outbox();
    }

    void test_append_entries_replication_commit_and_ordered_apply() {
        FakeClock clock;
        FixedTimeout leader_timeout(10), follower_timeout(100);
        MemoryPersistentStorage leader_storage, follower_storage;
        RecordingStateMachine leader_machine, follower_machine;
        RaftNode leader(1, {2}, {}, leader_storage, clock, leader_timeout, &leader_machine);
        RaftNode follower(2, {1}, {}, follower_storage, clock, follower_timeout, &follower_machine);
        elect_two_node_leader(leader, follower, clock, 1, 2, 10);

        const LogIndex first = leader.append_command({0xA1});
        require(first == 1 && leader_storage.entries.size() == 1 &&
            leader_storage.entries[0] == LogEntry{leader.current_term(), {0xA1}},
            "leader durably appends before replication");
        auto outgoing = leader.drain_outbox();
        require(outgoing.size() == 1 && std::holds_alternative<AppendEntriesRequest>(outgoing[0].message),
            "leader sends AppendEntries");
        follower.receive(1, outgoing[0].message);
        require(follower_storage.entries == leader_storage.entries,
            "follower persists entry before successful response");
        auto responses = follower.drain_outbox();
        require(responses.size() == 1 && std::get<AppendEntriesResponse>(responses[0].message).success,
            "follower acknowledges after durable log save");
        require(leader.commit_index() == 0 && leader_machine.applied.empty(),
            "dropped AppendEntries response cannot advance leader commit");

        const auto success = std::get<AppendEntriesResponse>(responses[0].message);
        leader.receive(2, success);
        require(leader.commit_index() == 1 && leader.last_applied() == 1 &&
            leader_machine.applied.size() == 1 && leader_machine.applied[0].first == 1,
            "majority response commits and applies in index order");
        outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "leader advertises the updated commit index");
        const auto commit_heartbeat = std::get<AppendEntriesRequest>(outgoing[0].message);
        require(commit_heartbeat.entries.empty() && commit_heartbeat.leader_commit == 1,
            "empty AppendEntries carries commit heartbeat");
        follower.receive(1, commit_heartbeat);
        require(follower.commit_index() == 1 && follower.last_applied() == 1 &&
            follower_machine.applied.size() == 1 && follower_machine.applied[0].first == 1,
            "follower applies only after leader_commit advances");

        leader.receive(2, AppendEntriesResponse{leader.current_term(), false, 0, 1, 0});
        require(leader.match_index(2) == 1 && leader.next_index(2) == 2,
            "delayed stale failure cannot undo newer replication progress");
    }

        void test_periodic_heartbeat_and_higher_term_replication_response() {
        FakeClock clock;
        FixedTimeout election_timeout(10);
        MemoryPersistentStorage leader_storage, follower_storage;
        RaftNode leader(1, {2}, {}, leader_storage, clock, election_timeout, nullptr, 5);
        RaftNode follower(2, {1}, {}, follower_storage, clock, election_timeout);
        elect_two_node_leader(leader, follower, clock, 1, 2, 10);
        clock.advance_to(15);
        leader.poll();
        const auto heartbeat = leader.drain_outbox();
        require(heartbeat.size() == 1 && std::holds_alternative<AppendEntriesRequest>(heartbeat[0].message),
            "expired heartbeat timer emits AppendEntries");
        const auto& append = std::get<AppendEntriesRequest>(heartbeat[0].message);
        require(append.entries.empty() && append.prev_log_index == 0 && append.prev_log_term == 0,
            "periodic heartbeat has empty entries and valid empty-prefix metadata");
        follower.receive(1, heartbeat[0].message);
        const auto ack = follower.drain_outbox();
        require(ack.size() == 1 && std::get<AppendEntriesResponse>(ack[0].message).success,
            "follower acknowledges heartbeat after prefix validation");
        leader.receive(2, AppendEntriesResponse{leader.current_term() + 1, false, 0, 1, 0});
        require(leader.role() == Role::Follower && leader.current_term() == 2 &&
                leader_storage.state.current_term == 2,
            "higher-term replication response durably forces leader step-down");
        }

        void test_simulated_network_reorders_append_batches_and_responses() {
        FakeClock clock;
        FixedTimeout timeout(10);
        MemoryPersistentStorage leader_storage, follower_storage;
        RecordingStateMachine leader_machine, follower_machine;
        RaftNode leader(1, {2}, {}, leader_storage, clock, timeout, &leader_machine);
        RaftNode follower(2, {1}, {}, follower_storage, clock, timeout, &follower_machine);
        SimulatedNetwork network;
        network.add(leader);
        network.add(follower);
        clock.advance_to(10);
        leader.poll();
        network.collect_outboxes();
        require(network.deliver_next(), "simulated network delivers election request");
        require(network.deliver_next(), "simulated network returns vote response");
        require(leader.role() == Role::Leader, "simulated network elects leader");
        network.collect_outboxes();
        require(network.deliver_next(), "simulated network delivers initial heartbeat");
        require(network.deliver_next(), "simulated network delivers heartbeat response");

        (void)leader.append_command({0x01});
        (void)leader.append_command({0x02});
        network.collect_outboxes();
        require(network.queued() == 2, "two append batches are simultaneously in flight");
        require(network.deliver_at(1), "deliver later append batch before earlier one");
        require(follower.log().size() == 2, "later batch replicates both ordered entries");
        require(network.deliver_at(1), "deliver later success response before earlier request");
        require(leader.commit_index() == 2 && leader_machine.applied.size() == 2,
            "later response commits entries in order");
        require(network.deliver_at(0), "deliver delayed earlier append request");
        require(follower.log().size() == 2 && follower.log()[1].command == std::vector<std::uint8_t>{0x02},
            "delayed older append does not truncate a matching newer suffix");
        require(network.deliver_next(), "deliver delayed stale append response");
        require(leader.match_index(2) == 2 && leader.commit_index() == 2 && follower.log() == leader.log(),
            "reordered messages preserve match, commit, and log-prefix safety");
        }

        void test_divergent_log_conflict_resolution_and_current_term_commit_rule() {
        FakeClock clock;
        FixedTimeout leader_timeout(10), follower_timeout(100);
        MemoryPersistentStorage leader_storage, follower_storage;
        leader_storage.state.current_term = 2;
        follower_storage.state.current_term = 2;
        leader_storage.entries = {{1, {0x11}}, {2, {0x22}}, {2, {0x23}}};
        follower_storage.entries = {{1, {0x11}}, {1, {0xE2}}, {1, {0xE3}}};
        RecordingStateMachine leader_machine, follower_machine;
        RaftNode leader(1, {2}, {2, 3}, leader_storage, clock, leader_timeout, &leader_machine);
        RaftNode follower(2, {1}, {1, 3}, follower_storage, clock, follower_timeout, &follower_machine);
        elect_two_node_leader(leader, follower, clock, 1, 2, 10, false);
        require(leader.current_term() == 3, "new leader starts a new term");

        auto outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "leader probes follower with last-log heartbeat");
        follower.receive(1, outgoing[0].message);
        auto response = follower.drain_outbox();
        require(response.size() == 1 && !std::get<AppendEntriesResponse>(response[0].message).success,
            "prev-log-term mismatch rejected");
        require(follower_storage.entries[1].command == std::vector<std::uint8_t>{0xE2},
            "mismatch alone does not destructively truncate follower log");

        leader.receive(2, std::get<AppendEntriesResponse>(response[0].message));
        outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "leader backs nextIndex to conflict boundary");
        const auto repair = std::get<AppendEntriesRequest>(outgoing[0].message);
        require(repair.prev_log_index == 1 && repair.entries.size() == 2,
            "leader retries from first conflicting term boundary");
        follower.receive(1, repair);
        require(follower_storage.entries == leader_storage.entries,
            "follower truncates only conflicting uncommitted suffix and appends leader entries");
        response = follower.drain_outbox();
        require(response.size() == 1 && std::get<AppendEntriesResponse>(response[0].message).success,
            "repaired log acknowledged");
        leader.receive(2, std::get<AppendEntriesResponse>(response[0].message));
        require(leader.match_index(2) == 3 && leader.commit_index() == 0 && leader_machine.applied.empty(),
            "old-term entries replicated on majority are not committed by themselves");
        (void)leader.drain_outbox();

        const LogIndex current_term_index = leader.append_command({0x33});
        require(current_term_index == 4, "new current-term entry appended after repaired prefix");
        outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "new entry replicated");
        follower.receive(1, outgoing[0].message);
        response = follower.drain_outbox();
        leader.receive(2, std::get<AppendEntriesResponse>(response[0].message));
        require(leader.commit_index() == 4 && leader_machine.applied.size() == 4,
            "current-term entry majority commits the preceding old-term prefix");
        for (std::size_t i = 0; i < leader_machine.applied.size(); ++i)
        require(leader_machine.applied[i].first == i + 1, "leader applies entries in ascending index order");
        outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "leader propagates commit after current-term quorum");
        follower.receive(1, outgoing[0].message);
        require(follower.commit_index() == 4 && follower_machine.applied.size() == 4,
            "follower applies complete committed prefix in order");
    }

    void test_isolated_leader_cannot_commit_and_healing_replaces_only_uncommitted_entry() {
        FakeClock clock;
        FixedTimeout timeout(100), fast_timeout(5);
        MemoryPersistentStorage s1, s2, s3;
        RecordingStateMachine m1, m2, m3;
        RaftNode old_leader(1, {2, 3}, {}, s1, clock, timeout, &m1);
        RaftNode node2(2, {1, 3}, {}, s2, clock, fast_timeout, &m2);
        RaftNode node3(3, {1, 2}, {}, s3, clock, timeout, &m3);
        // Elect node 1 using node 2, but drop all subsequent network messages.
        clock.advance_to(100);
        old_leader.poll();
        auto requests = old_leader.drain_outbox();
        node2.receive(1, requests.front().message);
        auto votes = node2.drain_outbox();
        old_leader.receive(2, votes.front().message);
        require(old_leader.role() == Role::Leader, "initial leader elected");
        (void)old_leader.drain_outbox(); // intentionally drop leader heartbeats

        (void)old_leader.append_command({0xA0});
        auto isolated_append = old_leader.drain_outbox();
        require(old_leader.commit_index() == 0 && m1.applied.empty(),
            "isolated leader cannot commit its unreplicated entry");
        (void)isolated_append; // dropped by partition

        // Node 2 starts term 2 and wins with node 3; its empty log is freshest among these followers.
        clock.advance_to(105);
        node2.poll();
        requests = node2.drain_outbox();
        const auto vote_request = std::find_if(requests.begin(), requests.end(), [](const OutboundMessage& item) {
        return item.target == 3 && std::holds_alternative<RequestVoteRequest>(item.message);
        });
        require(vote_request != requests.end(), "partitioned follower starts new election");
        node3.receive(2, vote_request->message);
        votes = node3.drain_outbox();
        node2.receive(3, votes.front().message);
        require(node2.role() == Role::Leader && node2.current_term() == 2,
            "connected majority elects a higher-term leader");
        (void)node2.drain_outbox();
        (void)node3.drain_outbox();

        (void)node2.append_command({0xB0});
        auto replication = node2.drain_outbox();
        const auto to3 = std::find_if(replication.begin(), replication.end(), [](const OutboundMessage& item) {
        return item.target == 3;
        });
        require(to3 != replication.end(), "new leader replicates to its connected peer");
        node3.receive(2, to3->message);
        auto append_response = node3.drain_outbox();
        node2.receive(3, append_response.front().message);
        require(node2.commit_index() == 1 && node2.last_applied() == 1,
            "new leader commits only with its connected majority");
        require(old_leader.commit_index() == 0 && m1.applied.empty(),
            "isolated former leader has no independent commit authority");

        replication = node2.drain_outbox(); // leader commit heartbeat for node 3
        const auto commit_to3 = std::find_if(replication.begin(), replication.end(), [](const OutboundMessage& item) {
        return item.target == 3;
        });
        require(commit_to3 != replication.end(), "new leader broadcasts committed index");
        node3.receive(2, commit_to3->message);

        const auto reconnect = std::find_if(replication.begin(), replication.end(), [](const OutboundMessage& item) {
        return item.target == 1;
        });
        require(reconnect != replication.end(), "new leader has replication message for healed old leader");
        old_leader.receive(2, reconnect->message);
        require(old_leader.role() == Role::Follower && old_leader.current_term() == 2,
            "higher-term legitimate leader forces old leader to step down");
        require(s1.entries.size() == 1 && s1.entries[0].command == std::vector<std::uint8_t>{0xB0},
            "healing truncates old leader's uncommitted conflicting entry");
        require(old_leader.commit_index() == 1 && m1.applied.size() == 1 &&
                    m1.applied[0].second.command == std::vector<std::uint8_t>{0xB0},
                "former leader applies the replacement only after the new quorum commits it");
    }

    void test_committed_prefix_cannot_be_overwritten_by_later_leader() {
        FakeClock clock;
        FixedTimeout timeout(10);
        MemoryPersistentStorage s1, s2, s3;
        RecordingStateMachine m1, m2, m3;
        RaftNode first(1, {2, 3}, {}, s1, clock, timeout, &m1);
        RaftNode second(2, {1, 3}, {}, s2, clock, timeout, &m2);
        clock.advance_to(10);
        first.poll();
        auto requests = first.drain_outbox();
        second.receive(1, requests[0].message);
        auto responses = second.drain_outbox();
        first.receive(2, responses[0].message);
        require(first.role() == Role::Leader, "initial leader elected");
        (void)first.drain_outbox();
        (void)first.append_command({0xC1});
        auto appends = first.drain_outbox();
        second.receive(1, appends.front().message);
        responses = second.drain_outbox();
        first.receive(2, responses[0].message);
        require(first.commit_index() == 1, "entry committed by majority");
        appends = first.drain_outbox();
        const auto commit_msg = std::find_if(appends.begin(), appends.end(), [](const OutboundMessage& item) {
        return item.target == 2;
        });
        second.receive(1, commit_msg->message);
        require(second.commit_index() == 1 && s2.entries[0].command == std::vector<std::uint8_t>{0xC1},
            "follower retains and applies committed entry");

        MemoryPersistentStorage stale_storage;
        RecordingStateMachine stale_machine;
        stale_storage.state.current_term = 2;
        RaftNode stale_candidate(3, {1, 2}, {}, stale_storage, clock, timeout, &stale_machine);
        clock.advance_to(20);
        stale_candidate.poll(); // starts term 3 with an empty log
        requests = stale_candidate.drain_outbox();
        const auto ask_committed_follower = std::find_if(requests.begin(), requests.end(), [](const OutboundMessage& item) {
        return item.target == 2 && std::holds_alternative<RequestVoteRequest>(item.message);
        });
        if (ask_committed_follower != requests.end()) second.receive(3, ask_committed_follower->message);
        responses = second.drain_outbox();
        require(!responses.empty() && !std::get<RequestVoteResponse>(responses.back().message).vote_granted,
            "candidate missing committed prefix cannot win the follower's vote");

        bool conflict_rejected = false;
        try {
        second.receive(3, AppendEntriesRequest{3, 3, 0, 0, {{3, {0xFF}}}, 0});
        } catch (const RaftError&) { conflict_rejected = true; }
        require(conflict_rejected && s2.entries[0].command == std::vector<std::uint8_t>{0xC1} &&
            second.commit_index() == 1,
            "committed log entry cannot be truncated by a conflicting AppendEntries");
    }

    void test_large_log_catchup_uses_bounded_append_batches() {
        FakeClock clock;
        FixedTimeout leader_timeout(10), follower_timeout(100);
        MemoryPersistentStorage leader_storage, follower_storage;
        leader_storage.state.current_term = 1;
        for (std::uint8_t value = 1; value <= 40; ++value)
            leader_storage.entries.push_back(LogEntry{1, {value}});
        RaftNode leader(1, {2}, LogMetadata{1, 40}, leader_storage, clock, leader_timeout);
        RaftNode follower(2, {1}, {}, follower_storage, clock, follower_timeout);
        elect_two_node_leader(leader, follower, clock, 1, 2, 10, false);

        auto outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "new leader probes the follower's empty prefix");
        follower.receive(1, outgoing.front().message);
        auto response = follower.drain_outbox();
        require(response.size() == 1 && !std::get<AppendEntriesResponse>(response.front().message).success,
                "empty follower reports its conflict boundary");
        leader.receive(2, response.front().message);

        outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "leader starts bounded catch-up batch");
        const auto first_batch = std::get<AppendEntriesRequest>(outgoing.front().message);
        require(first_batch.entries.size() == 32, "append batch respects entry-count bound");
        follower.receive(1, first_batch);
        response = follower.drain_outbox();
        leader.receive(2, response.front().message);

        outgoing = leader.drain_outbox();
        require(outgoing.size() == 1, "successful batch schedules the remaining suffix");
        const auto second_batch = std::get<AppendEntriesRequest>(outgoing.front().message);
        require(second_batch.prev_log_index == 32 && second_batch.entries.size() == 8,
                "second append resumes exactly after the confirmed prefix");
        follower.receive(1, second_batch);
        response = follower.drain_outbox();
        leader.receive(2, response.front().message);
        require(follower.log() == leader.log() && leader.match_index(2) == 40,
                "bounded catch-up reproduces the complete ordered log");
    }

} // namespace

int main() {
    try {
        test_competing_candidates_split_votes();
        test_single_candidate_wins_majority_and_votes_persist_before_send();
        test_stale_candidate_is_denied();
        test_higher_term_steps_candidate_down();
        test_repeated_election_attempts_increment_term();
        test_restart_recovers_vote_and_rejects_second_candidate();
        test_higher_term_request_and_log_index_tie_break();
        test_storage_failure_prevents_election_messages();
        test_log_persistence_failure_prevents_replication_ack();
        test_stale_leader_append_entries_rejected();
        test_append_entries_replication_commit_and_ordered_apply();
        test_periodic_heartbeat_and_higher_term_replication_response();
        test_simulated_network_reorders_append_batches_and_responses();
        test_divergent_log_conflict_resolution_and_current_term_commit_rule();
        test_isolated_leader_cannot_commit_and_healing_replaces_only_uncommitted_entry();
        test_committed_prefix_cannot_be_overwritten_by_later_leader();
        test_large_log_catchup_uses_bounded_append_batches();
        std::cout << "atlas_raft_tests: all deterministic replication and safety tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas_raft_tests: " << error.what() << '\n';
        return 1;
    }
}
