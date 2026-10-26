#include "atlas/raft.hpp"

#include <algorithm>
#include <limits>
#include <set>
#include <type_traits>
#include <utility>

namespace atlas::raft {
namespace {

bool log_is_at_least_as_fresh(const LogMetadata& candidate, const LogMetadata& local) noexcept {
    return candidate.last_log_term != local.last_log_term
        ? candidate.last_log_term > local.last_log_term
        : candidate.last_log_index >= local.last_log_index;
}

} // namespace

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers, LogMetadata log_metadata,
                   PersistentStorage& storage, const Clock& clock, ElectionTimeoutSource& timeouts,
                   StateMachine* state_machine, Milliseconds heartbeat_interval_ms)
    : id_(id), peers_(std::move(peers)), log_metadata_(log_metadata), storage_(storage),
      clock_(clock), timeouts_(timeouts), persistent_(storage_.load()), log_(storage_.load_log()),
      state_machine_(state_machine), heartbeat_interval_ms_(heartbeat_interval_ms) {
    if (id_ == 0) throw RaftError("Raft node ID must be nonzero");
    if (heartbeat_interval_ms_ == 0) throw RaftError("heartbeat interval must be positive");
    std::set<NodeId> unique_peers;
    for (NodeId peer : peers_) {
        if (peer == 0 || peer == id_ || !unique_peers.insert(peer).second)
            throw RaftError("peer IDs must be unique, nonzero, and exclude this node");
    }
    if (persistent_.current_term == 0 && persistent_.voted_for.has_value())
        throw RaftError("term zero cannot contain a persisted vote");
    if (persistent_.voted_for.has_value() && *persistent_.voted_for != id_ && !is_peer(*persistent_.voted_for))
        throw RaftError("persistent vote references a node outside the voter set");
    commit_index_ = storage_.load_commit_index();
    if (commit_index_ > log_.size()) throw RaftError("persisted commit index exceeds the durable log");
    for (std::size_t index = 0; index < log_.size(); ++index) {
        const LogEntry& entry = log_[index];
        if (entry.term == 0 || entry.term > persistent_.current_term ||
            (index > 0 && entry.term < log_[index - 1].term)) {
            throw RaftError("persistent log has invalid or decreasing entry terms");
        }
    }
    refresh_log_metadata();
    if (log_metadata_.last_log_index != log_.size() ||
        (log_.empty() ? log_metadata_.last_log_term != 0
                      : log_metadata_.last_log_term != log_.back().term)) {
        throw RaftError("supplied log metadata does not match persistent log");
    }
    if (state_machine_) {
        state_machine_->restore_committed(std::vector<LogEntry>(log_.begin(),
            log_.begin() + static_cast<std::ptrdiff_t>(commit_index_)));
        last_applied_ = commit_index_;
    }
    reset_election_deadline();
}

void RaftNode::poll() {
    if (role_ == Role::Leader) {
        if (clock_.now_ms() >= heartbeat_deadline_) {
            send_append_entries_all();
            const Milliseconds now = clock_.now_ms();
            if (heartbeat_interval_ms_ > std::numeric_limits<Milliseconds>::max() - now)
                throw RaftError("heartbeat deadline is not representable");
            heartbeat_deadline_ = now + heartbeat_interval_ms_;
        }
    } else if (clock_.now_ms() >= election_deadline_) {
        start_election();
    }
}

std::vector<LogIndex> RaftNode::poll_applied() {
    const LogIndex before = last_applied_;
    poll();
    std::vector<LogIndex> newly_applied;
    for (LogIndex index = before + 1; index <= last_applied_; ++index) newly_applied.push_back(index);
    return newly_applied;
}

std::vector<LogIndex> RaftNode::receive_applied(NodeId from, const Message& message) {
    const LogIndex before = last_applied_;
    receive(from, message);
    std::vector<LogIndex> newly_applied;
    for (LogIndex index = before + 1; index <= last_applied_; ++index) newly_applied.push_back(index);
    return newly_applied;
}

std::vector<LogIndex> RaftNode::apply_committed() {
    const LogIndex before = last_applied_;
    while (last_applied_ < commit_index_) {
        const LogIndex index = last_applied_ + 1;
        if (state_machine_) state_machine_->apply(index, log_[index - 1]);
        last_applied_ = index;
    }
    std::vector<LogIndex> newly_applied;
    for (LogIndex index = before + 1; index <= last_applied_; ++index) newly_applied.push_back(index);
    return newly_applied;
}

void RaftNode::receive(NodeId from, const Message& message) {
    if (!is_peer(from)) return;
    std::visit([&](const auto& rpc) {
        using T = std::decay_t<decltype(rpc)>;
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
            if (rpc.candidate_id != from || rpc.term == 0 ||
                ((rpc.last_log_index == 0) != (rpc.last_log_term == 0)) || rpc.last_log_term > rpc.term) return;
            if (rpc.term < persistent_.current_term) {
                send(from, RequestVoteResponse{persistent_.current_term, false});
                return;
            }
            if (rpc.term > persistent_.current_term) become_follower(rpc.term);
            const bool can_vote = !persistent_.voted_for || *persistent_.voted_for == rpc.candidate_id;
            const bool fresh = log_is_at_least_as_fresh({rpc.last_log_term, rpc.last_log_index}, log_metadata_);
            const bool granted = can_vote && fresh;
            if (granted && persistent_.voted_for != rpc.candidate_id) {
                PersistentState next{persistent_.current_term, rpc.candidate_id};
                storage_.save(next);
                persistent_ = next;
                reset_election_deadline();
            } else if (granted) {
                reset_election_deadline();
            }
            send(from, RequestVoteResponse{persistent_.current_term, granted});
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
            if (rpc.term > persistent_.current_term) {
                become_follower(rpc.term);
                reset_election_deadline();
                return;
            }
            if (role_ != Role::Candidate || rpc.term != persistent_.current_term || !rpc.vote_granted) return;
            if (std::find(votes_received_.begin(), votes_received_.end(), from) == votes_received_.end()) {
                votes_received_.push_back(from);
                maybe_become_leader();
            }
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
            if (rpc.leader_id != from || rpc.term == 0 ||
                rpc.prev_log_term > rpc.term ||
                ((rpc.prev_log_index == 0) != (rpc.prev_log_term == 0))) return;
            receive_append_entries(from, rpc);
        } else {
            receive_append_entries_response(from, rpc);
        }
    }, message);
}

std::vector<OutboundMessage> RaftNode::drain_outbox() {
    std::vector<OutboundMessage> result;
    result.swap(outbox_);
    return result;
}

LogIndex RaftNode::append_command(std::vector<std::uint8_t> command) {
    if (role_ != Role::Leader) throw RaftError("only a leader may append commands");
    if (log_.size() == std::numeric_limits<LogIndex>::max()) throw RaftError("log index exhausted");
    std::vector<LogEntry> next_log = log_;
    next_log.push_back(LogEntry{persistent_.current_term, std::move(command)});
    storage_.save_log(next_log); // durable before this leader reports replication
    log_.swap(next_log);
    refresh_log_metadata();
    const LogIndex index = static_cast<LogIndex>(log_.size());
    match_index_[id_] = index;
    send_append_entries_all();
    advance_commit_index();
    return index;
}

std::optional<LogIndex> RaftNode::next_index(NodeId peer) const noexcept {
    const auto it = next_index_.find(peer);
    return it == next_index_.end() ? std::nullopt : std::optional<LogIndex>(it->second);
}

std::optional<LogIndex> RaftNode::match_index(NodeId peer) const noexcept {
    const auto it = match_index_.find(peer);
    return it == match_index_.end() ? std::nullopt : std::optional<LogIndex>(it->second);
}

void RaftNode::reset_election_deadline() {
    const Milliseconds timeout = timeouts_.next_timeout_ms();
    const Milliseconds now = clock_.now_ms();
    if (timeout == 0 || timeout > std::numeric_limits<Milliseconds>::max() - now)
        throw RaftError("election timeout must be positive and deadline representable");
    election_deadline_ = now + timeout;
}

void RaftNode::become_follower(Term term) {
    if (term < persistent_.current_term) return;
    if (term > persistent_.current_term) {
        PersistentState next{term, std::nullopt};
        storage_.save(next); // durable before processing the higher-term RPC
        persistent_ = next;
    }
    role_ = Role::Follower;
    votes_received_.clear();
    next_index_.clear();
    match_index_.clear();
}

void RaftNode::start_election() {
    if (persistent_.current_term == std::numeric_limits<Term>::max()) throw RaftError("Raft term exhausted");
    PersistentState next{persistent_.current_term + 1, id_};
    storage_.save(next);
    persistent_ = next;
    role_ = Role::Candidate;
    votes_received_ = {id_};
    const Milliseconds timeout = timeouts_.next_timeout_ms();
    const Milliseconds now = clock_.now_ms();
    if (timeout == 0 || timeout > std::numeric_limits<Milliseconds>::max() - now)
        throw RaftError("election timeout must be positive and deadline representable");
    election_deadline_ = now + timeout;
    maybe_become_leader();
    if (role_ == Role::Candidate) {
        const RequestVoteRequest request{persistent_.current_term, id_, log_metadata_.last_log_index,
                                         log_metadata_.last_log_term};
        for (NodeId peer : peers_) send(peer, request);
    }
}

void RaftNode::maybe_become_leader() {
    const std::size_t majority = (peers_.size() + 1) / 2 + 1;
    if (role_ != Role::Candidate || votes_received_.size() < majority) return;
    role_ = Role::Leader;
    votes_received_.clear();
    const LogIndex next = static_cast<LogIndex>(log_.size()) + 1;
    next_index_.clear();
    match_index_.clear();
    for (NodeId peer : peers_) {
        next_index_[peer] = next;
        match_index_[peer] = 0;
    }
    match_index_[id_] = static_cast<LogIndex>(log_.size());
    const Milliseconds now = clock_.now_ms();
    if (heartbeat_interval_ms_ > std::numeric_limits<Milliseconds>::max() - now)
        throw RaftError("heartbeat deadline is not representable");
    heartbeat_deadline_ = now + heartbeat_interval_ms_;
    send_append_entries_all();
}

void RaftNode::receive_append_entries(NodeId from, const AppendEntriesRequest& request) {
    if (request.term < persistent_.current_term) {
        send(from, AppendEntriesResponse{persistent_.current_term, false, 0,
                                         static_cast<LogIndex>(log_.size() + 1), 0});
        return;
    }
    if (request.term > persistent_.current_term) become_follower(request.term);
    else if (role_ != Role::Follower) {
        role_ = Role::Follower;
        votes_received_.clear();
        next_index_.clear();
        match_index_.clear();
    }
    reset_election_deadline();

    if (request.prev_log_index > log_.size()) {
        send(from, AppendEntriesResponse{persistent_.current_term, false, 0,
                                         static_cast<LogIndex>(log_.size() + 1), 0});
        return;
    }
    if (request.entries.size() > std::numeric_limits<LogIndex>::max() - request.prev_log_index) {
        send(from, AppendEntriesResponse{persistent_.current_term, false, 0,
                                         static_cast<LogIndex>(log_.size() + 1), 0});
        return;
    }
    const Term prefix_term = request.prev_log_index == 0 ? 0 : log_[request.prev_log_index - 1].term;
    if (request.prev_log_term != prefix_term) {
        const Term conflict_term = prefix_term;
        LogIndex first = request.prev_log_index;
        while (first > 1 && log_[first - 2].term == conflict_term) --first;
        send(from, AppendEntriesResponse{persistent_.current_term, false, 0,
                                         first == 0 ? 1 : first, conflict_term});
        return;
    }
    std::vector<LogEntry> next_log = log_;
    std::size_t incoming = 0;
    LogIndex index = request.prev_log_index + 1;
    Term previous_entry_term = prefix_term;
    for (; incoming < request.entries.size(); ++incoming, ++index) {
        const LogEntry& entry = request.entries[incoming];
        if (entry.term == 0 || entry.term > request.term ||
            entry.term < previous_entry_term) {
            send(from, AppendEntriesResponse{persistent_.current_term, false, 0,
                                             static_cast<LogIndex>(log_.size() + 1), 0});
            return;
        }
        previous_entry_term = entry.term;
        if (index <= next_log.size()) {
            if (next_log[index - 1].term == entry.term) continue;
            if (index <= commit_index_) {
                throw RaftError("AppendEntries conflicts with a committed log entry");
            }
            next_log.resize(static_cast<std::size_t>(index - 1));
        }
        next_log.push_back(entry);
    }

    if (next_log != log_) {
        storage_.save_log(next_log); // persist truncation and additions before success response
        log_.swap(next_log);
        refresh_log_metadata();
    }
    const LogIndex matched = request.prev_log_index + request.entries.size();
    const LogIndex new_commit = std::max(commit_index_, std::min(request.leader_commit, matched));
    if (new_commit > commit_index_) {
        storage_.save_commit_index(new_commit);
        commit_index_ = new_commit;
    }
    apply_committed_entries();
    send(from, AppendEntriesResponse{persistent_.current_term, true, matched, 0, 0});
}

void RaftNode::receive_append_entries_response(NodeId from, const AppendEntriesResponse& response) {
    if (response.term > persistent_.current_term) {
        become_follower(response.term);
        reset_election_deadline();
        return;
    }
    if (role_ != Role::Leader || response.term != persistent_.current_term) return;
    auto next_it = next_index_.find(from);
    auto match_it = match_index_.find(from);
    if (next_it == next_index_.end() || match_it == match_index_.end()) return;
    if (response.success) {
        if (response.match_index > log_.size() || response.match_index + 1 < next_it->second) return;
        match_it->second = std::max(match_it->second, response.match_index);
        next_it->second = match_it->second + 1;
        advance_commit_index();
        if (next_it->second <= log_.size()) send_append_entries(from);
    } else {
        if (next_it->second <= match_it->second + 1) return; // ignore delayed failure after newer success
        LogIndex proposed = response.conflict_index;
        if (response.conflict_term != 0) {
            for (LogIndex index = static_cast<LogIndex>(log_.size()); index > 0; --index) {
                if (log_[index - 1].term == response.conflict_term) {
                    proposed = index + 1;
                    break;
                }
            }
        }
        if (proposed == 0) proposed = 1;
        proposed = std::min(proposed, static_cast<LogIndex>(log_.size()) + 1);
        proposed = std::max(proposed, match_it->second + 1);
        if (proposed >= next_it->second && next_it->second > 1) proposed = next_it->second - 1;
        next_it->second = proposed;
        send_append_entries(from);
    }
}

void RaftNode::send_append_entries(NodeId peer) {
    constexpr std::size_t max_batch_entries = 32;
    constexpr std::size_t max_batch_bytes = 3000;
    const auto next_it = next_index_.find(peer);
    if (next_it == next_index_.end()) return;
    const LogIndex next = std::max<LogIndex>(1, next_it->second);
    const LogIndex previous = next - 1;
    const Term previous_term = previous == 0 ? 0 : log_[previous - 1].term;
    std::vector<LogEntry> entries;
    if (next <= log_.size()) {
        std::size_t batch_bytes = 0;
        for (auto it = log_.begin() + static_cast<std::ptrdiff_t>(next - 1);
             it != log_.end() && entries.size() < max_batch_entries; ++it) {
            const std::size_t entry_bytes = it->command.size() > max_batch_bytes - 12
                ? max_batch_bytes : 12 + it->command.size();
            if (!entries.empty() && entry_bytes > max_batch_bytes - batch_bytes) break;
            entries.push_back(*it);
            batch_bytes += entry_bytes;
            if (batch_bytes >= max_batch_bytes) break;
        }
    }
    send(peer, AppendEntriesRequest{persistent_.current_term, id_, previous, previous_term,
                                    std::move(entries), commit_index_});
}

void RaftNode::send_append_entries_all() {
    for (NodeId peer : peers_) send_append_entries(peer);
}

void RaftNode::advance_commit_index() {
    if (role_ != Role::Leader) return;
    for (LogIndex candidate = static_cast<LogIndex>(log_.size()); candidate > commit_index_; --candidate) {
        if (log_[candidate - 1].term != persistent_.current_term) continue;
        std::size_t replicas = 0;
        for (NodeId peer : peers_) {
            const auto match = match_index_.find(peer);
            if (match != match_index_.end() && match->second >= candidate) ++replicas;
        }
        if (match_index_[id_] >= candidate) ++replicas;
        if (replicas >= (peers_.size() + 1) / 2 + 1) {
            storage_.save_commit_index(candidate);
            commit_index_ = candidate;
            apply_committed_entries();
            send_append_entries_all();
            break;
        }
    }
}

void RaftNode::apply_committed_entries() {
    (void)apply_committed();
}

void RaftNode::refresh_log_metadata() noexcept {
    log_metadata_.last_log_index = static_cast<LogIndex>(log_.size());
    log_metadata_.last_log_term = log_.empty() ? 0 : log_.back().term;
}

void RaftNode::send(NodeId target, Message message) {
    outbox_.push_back(OutboundMessage{target, std::move(message)});
}

bool RaftNode::is_peer(NodeId peer) const noexcept {
    return std::find(peers_.begin(), peers_.end(), peer) != peers_.end();
}

} // namespace atlas::raft
