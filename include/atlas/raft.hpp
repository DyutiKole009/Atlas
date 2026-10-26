#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <map>
#include <optional>
#include <stdexcept>
#include <variant>
#include <vector>

namespace atlas::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;
using Milliseconds = std::uint64_t;

struct PersistentState {
    Term current_term{};
    std::optional<NodeId> voted_for;
};

struct LogMetadata {
    Term last_log_term{};
    LogIndex last_log_index{};
};

struct LogEntry {
    Term term{};
    std::vector<std::uint8_t> command;
    friend bool operator==(const LogEntry&, const LogEntry&) = default;
};

struct RequestOutcome {
    std::uint64_t client_id{};
    std::uint64_t request_id{};
    std::vector<std::uint8_t> request_fingerprint;
    std::vector<std::uint8_t> encoded_response;
    friend bool operator==(const RequestOutcome&, const RequestOutcome&) = default;
};

enum class Role { Follower, Candidate, Leader };

struct RequestVoteRequest {
    Term term{};
    NodeId candidate_id{};
    LogIndex last_log_index{};
    Term last_log_term{};
};

struct RequestVoteResponse {
    Term term{};
    bool vote_granted{false};
};

struct AppendEntriesRequest {
    Term term{};
    NodeId leader_id{};
    LogIndex prev_log_index{};
    Term prev_log_term{};
    std::vector<LogEntry> entries; // empty entries vector is a heartbeat
    LogIndex leader_commit{};
};

struct AppendEntriesResponse {
    Term term{};
    bool success{false};
    LogIndex match_index{};
    LogIndex conflict_index{};
    Term conflict_term{};
};

using Message = std::variant<RequestVoteRequest, RequestVoteResponse,
                             AppendEntriesRequest, AppendEntriesResponse>;

struct OutboundMessage {
    NodeId target{};
    Message message;
};

class RaftError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class Clock {
public:
    virtual ~Clock() = default;
    [[nodiscard]] virtual Milliseconds now_ms() const noexcept = 0;
};

class ElectionTimeoutSource {
public:
    virtual ~ElectionTimeoutSource() = default;
    // Production sources should randomize this duration within configured Raft election bounds.
    [[nodiscard]] virtual Milliseconds next_timeout_ms() = 0;
};

class PersistentStorage {
public:
    virtual ~PersistentStorage() = default;
    [[nodiscard]] virtual PersistentState load() = 0;
    // Must atomically and durably store the complete state, or throw without claiming success.
    virtual void save(const PersistentState& state) = 0;
    [[nodiscard]] virtual std::vector<LogEntry> load_log() = 0;
    // Replaces the complete contiguous log atomically and durably, or throws without claiming success.
    virtual void save_log(const std::vector<LogEntry>& entries) = 0;
    [[nodiscard]] virtual LogIndex load_commit_index() = 0;
    // Persist committed progress before state-machine application; a fresh deterministic state machine
    // can then replay exactly [1, commit_index] during recovery.
    virtual void save_commit_index(LogIndex commit_index) = 0;
    [[nodiscard]] virtual std::vector<RequestOutcome> load_request_outcomes() = 0;
    virtual void save_request_outcome(const RequestOutcome& outcome) = 0;
};

class StateMachine {
public:
    virtual ~StateMachine() = default;
    // Called exactly once per index during a node process lifetime, in contiguous index order.
    virtual void apply(LogIndex index, const LogEntry& entry) = 0;
    virtual void restore_committed(const std::vector<LogEntry>& entries) {
        for (std::size_t i = 0; i < entries.size(); ++i) apply(static_cast<LogIndex>(i + 1), entries[i]);
    }
};

class RaftNode {
public:
    RaftNode(NodeId id, std::vector<NodeId> peers, LogMetadata log_metadata,
             PersistentStorage& storage, const Clock& clock, ElectionTimeoutSource& timeouts,
             StateMachine* state_machine = nullptr, Milliseconds heartbeat_interval_ms = 50);

    // All calls are serialized by the owner of this RaftNode. There is no internal timer/network thread.
    void poll();
    void receive(NodeId from, const Message& message);
    // Process one serialized turn and return newly applied entries. Timer polling remains injectable.
    [[nodiscard]] std::vector<LogIndex> poll_applied();
    [[nodiscard]] std::vector<LogIndex> receive_applied(NodeId from, const Message& message);
    [[nodiscard]] std::vector<LogIndex> apply_committed();
    [[nodiscard]] std::vector<OutboundMessage> drain_outbox();
    // Leaders durably append before broadcasting; returns the assigned 1-based log index.
    [[nodiscard]] LogIndex append_command(std::vector<std::uint8_t> command);

    [[nodiscard]] NodeId id() const noexcept { return id_; }
    [[nodiscard]] Role role() const noexcept { return role_; }
    [[nodiscard]] Term current_term() const noexcept { return persistent_.current_term; }
    [[nodiscard]] std::optional<NodeId> voted_for() const noexcept { return persistent_.voted_for; }
    [[nodiscard]] std::size_t votes_received() const noexcept { return votes_received_.size(); }
    [[nodiscard]] LogMetadata log_metadata() const noexcept { return log_metadata_; }
    [[nodiscard]] const std::vector<LogEntry>& log() const noexcept { return log_; }
    [[nodiscard]] LogIndex commit_index() const noexcept { return commit_index_; }
    [[nodiscard]] LogIndex last_applied() const noexcept { return last_applied_; }
    [[nodiscard]] std::optional<LogIndex> next_index(NodeId peer) const noexcept;
    [[nodiscard]] std::optional<LogIndex> match_index(NodeId peer) const noexcept;

private:
    void reset_election_deadline();
    void become_follower(Term term);
    void start_election();
    void maybe_become_leader();
    void receive_append_entries(NodeId from, const AppendEntriesRequest& request);
    void receive_append_entries_response(NodeId from, const AppendEntriesResponse& response);
    void send_append_entries(NodeId peer);
    void send_append_entries_all();
    void advance_commit_index();
    void apply_committed_entries();
    void refresh_log_metadata() noexcept;
    void send(NodeId target, Message message);
    [[nodiscard]] bool is_peer(NodeId id) const noexcept;

    NodeId id_{};
    std::vector<NodeId> peers_;
    LogMetadata log_metadata_;
    PersistentStorage& storage_;
    const Clock& clock_;
    ElectionTimeoutSource& timeouts_;
    PersistentState persistent_;
    std::vector<LogEntry> log_;
    Role role_{Role::Follower};
    Milliseconds election_deadline_{};
    std::vector<NodeId> votes_received_;
    std::vector<OutboundMessage> outbox_;
    std::map<NodeId, LogIndex> next_index_;
    std::map<NodeId, LogIndex> match_index_;
    LogIndex commit_index_{};
    LogIndex last_applied_{};
    StateMachine* state_machine_{};
    Milliseconds heartbeat_interval_ms_{};
    Milliseconds heartbeat_deadline_{};
};

} // namespace atlas::raft
