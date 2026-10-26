#pragma once

#include "atlas/protocol.hpp"
#include "atlas/raft.hpp"

namespace atlas::raft {

[[nodiscard]] protocol::Message to_protocol_message(const Message& message);
[[nodiscard]] Message from_protocol_message(const protocol::Message& message);

} // namespace atlas::raft
