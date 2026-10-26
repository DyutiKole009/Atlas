#include "atlas/raft_transport.hpp"

#include <type_traits>

namespace atlas::raft {

protocol::Message to_protocol_message(const Message& message) {
    return std::visit([](const auto& rpc) -> protocol::Message {
        using T = std::decay_t<decltype(rpc)>;
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
            return protocol::RaftRequestVote{rpc.term, rpc.candidate_id, rpc.last_log_index, rpc.last_log_term};
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
            return protocol::RaftRequestVoteResponse{rpc.term, rpc.vote_granted};
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
            protocol::RaftAppendEntries result{rpc.term, rpc.leader_id, rpc.prev_log_index, rpc.prev_log_term, {}, rpc.leader_commit};
            result.entries.reserve(rpc.entries.size());
            for (const auto& entry : rpc.entries) result.entries.push_back({entry.term, entry.command});
            return result;
        } else {
            return protocol::RaftAppendEntriesResponse{rpc.term, rpc.success, rpc.match_index,
                                                       rpc.conflict_index, rpc.conflict_term};
        }
    }, message);
}

Message from_protocol_message(const protocol::Message& message) {
    return std::visit([](const auto& rpc) -> Message {
        using T = std::decay_t<decltype(rpc)>;
        if constexpr (std::is_same_v<T, protocol::RaftRequestVote>) {
            return RequestVoteRequest{rpc.term, rpc.candidate_id, rpc.last_log_index, rpc.last_log_term};
        } else if constexpr (std::is_same_v<T, protocol::RaftRequestVoteResponse>) {
            return RequestVoteResponse{rpc.term, rpc.vote_granted};
        } else if constexpr (std::is_same_v<T, protocol::RaftAppendEntries>) {
            AppendEntriesRequest result{rpc.term, rpc.leader_id, rpc.prev_log_index, rpc.prev_log_term, {}, rpc.leader_commit};
            result.entries.reserve(rpc.entries.size());
            for (const auto& entry : rpc.entries) result.entries.push_back({entry.term, entry.command});
            return result;
        } else if constexpr (std::is_same_v<T, protocol::RaftAppendEntriesResponse>) {
            return AppendEntriesResponse{rpc.term, rpc.success, rpc.match_index, rpc.conflict_index, rpc.conflict_term};
        } else {
            throw RaftError("client protocol message is not a Raft RPC");
        }
    }, message);
}

} // namespace atlas::raft
