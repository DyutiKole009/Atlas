# Project Atlas Raft Replication Core — Design v2

**Status:** an experimental fixed-three-node Raft integration, with a disk-backed store, TCP transport, deterministic exchange state machine, and a three-node integration test. Test results cover exercised traces; they are not a formal proof of Raft safety or liveness and are not a production certification.

## Existing module boundaries

Atlas has distinct modules for matching/storage (`atlas_matching`), protocol (`atlas_protocol`), the POSIX client/server (`atlas_node_lib`), and Raft (`atlas_raft`). `RaftNode` stores opaque command bytes and does not know order-book rules. Its typed RPC outbox is translated by `raft_transport` to protocol messages; `TcpNode` envelopes and sends these over the node's TCP listener. In replicated mode a single execution thread owns Raft and the exchange state machine. The fixed voter set is exactly three IDs. Persistent term/vote, log, commit index, and deterministic client outcomes use each process's own `FilePersistentStorage` directory.

## Beginner's guide to Raft concepts

- **Term:** a numbered election round. Terms only increase. A message from a higher term tells a node that its view is old, so it persists the new term and steps down.
- **Follower:** waits for a leader's messages or an election timeout.
- **Candidate:** starts an election, votes for itself, and asks every peer for one vote.
- **Leader:** a candidate that got votes from a strict majority. It replicates log entries; it does not become safe to commit an entry merely by declaring itself leader.
- **Log entry:** an ordered command tagged with the term in which the leader created it. The index is its 1-based position. This milestone stores opaque bytes; it does not interpret exchange commands.
- **RequestVote:** asks a server to vote. A server votes at most once in a term and only for a candidate whose log is at least as up-to-date: compare last-log term first, then index.
- **AppendEntries:** the leader sends a log suffix plus the preceding entry's index and term. The preceding pair is a consistency check: the receiver accepts the suffix only if it has that same prefix point. An empty suffix is a heartbeat and can also advance the follower's commit index.
- Catch-up AppendEntries use bounded batches (at most 32 entries and approximately 3,000 encoded entry bytes); each successful response drives the next batch. Heartbeats remain empty batches.
- **Conflict repair:** when the prefix check fails, the follower reports where the conflicting term begins; the leader backs up `nextIndex` and retries. A follower replaces only a conflicting **uncommitted** suffix. Committed entries are never truncated.
- **`nextIndex` and `matchIndex`:** for each follower, the leader's next send position and highest index that follower has confirmed. A response can increase progress, but a delayed failure cannot undo already-confirmed progress.
- **Commit index:** the highest entry known committed. A majority is necessary, but not sufficient to directly commit an arbitrary old-term entry: the leader advances commit only when a majority stores an entry from its **current term**. Earlier entries become committed as part of that prefix.
- **State-machine application:** commitment and execution are separate. The node applies committed entries one at a time from `lastApplied + 1` to `commitIndex`; replication alone does not execute commands.

## Supported protocol and state

The fixed voter set is configured at construction; there is no membership change.

**Persistent state and recovery:**

- `currentTerm` and `votedFor` are stored through `PersistentStorage::save()` before a vote grant, election RequestVote, or higher-term processing that depends on them.
- The complete contiguous log is loaded through `load_log()` and replaced through `save_log()`. `save_log()` must make the replacement atomic and durable or throw. Leaders durably append locally before sending replication. Followers durably persist truncation/appended entries before emitting a successful AppendEntries response. That success is the storage acknowledgement; it is not a client commit acknowledgement.
- `FilePersistentStorage` atomically republishes the term/vote, complete log, commit index, and request-outcome records in a checksummed file; its updates synchronize the temporary file and containing directory. A process must be the sole owner of its directory. Each of the three nodes must use a different directory.

On startup, the node reads the durable commit index and rebuilds a fresh matching state machine by replaying exactly the committed log prefix in contiguous index order. Replay verifies terminal response records against deterministic execution. The commit index is persisted before state-machine application; durable outcomes are written as application occurs, so replay is idempotent after a crash between those writes. Raft log compaction and snapshots are not implemented: the complete log is retained and replay time/storage grow with history.

**Volatile state:** role, election/heartbeat deadlines, candidate vote set, leader `nextIndex`/`matchIndex`, and `lastApplied` (reconstructed from the committed prefix on startup). An uncommitted command may be lost or overwritten after a leadership change. The old leader fails its waiting client with `InternalError` rather than claiming success; the client must retry the same request key against a node that can lead. The request may nevertheless later commit, so a retry can return its recorded result. No exactly-once external side effects beyond the deterministic exchange state machine are claimed.

**RPCs:** `RequestVoteRequest/Response` and `AppendEntriesRequest/Response`. AppendEntries responses report term, success, confirmed match index, and conflict index/term. Every higher-term RPC/response causes durable term update and step-down before proceeding. Stale-term leader AppendEntries is rejected. Same-term AppendEntries makes a candidate or former leader a follower and resets its election timeout.

**Timers:** an injected monotonic `Clock`, injected election-timeout source, and configured heartbeat interval. `poll()` handles expired election and heartbeat deadlines. There are no internal sleeps or timer threads. Real deployments must randomize election timeouts and choose heartbeat/election intervals appropriate to their transport.

**Deterministic simulated network:** tests hold an explicit queue of `{from,to,message}` envelopes and choose which to deliver, reorder, or drop. Fake time advances only when a test asks it to. This exercises delayed, duplicated, lost, and partitioned traces without TCP or arbitrary sleeps.

## Storage and acknowledgement contract

`PersistentStorage::save(state)`, `save_log(entries)`, `save_commit_index(index)`, and `save_request_outcome(outcome)` must return only after atomic durable publication or throw. The supplied file store uses `fsync` and atomic rename; this expresses the local filesystem durability request but does not prove survival of arbitrary device/controller/filesystem failures. Followers persist an entry before their successful AppendEntries response. Leaders persist a new command before replication and persist commit progress before applying it. A client receives a successful response only after a majority has stored the command, commit progress is durably advanced, and that node has applied the command. A follower or candidate returns `NotLeader` and never appends client writes.

This acknowledgement path adds at least a leader/follower network round trip, leader and follower storage synchronization, and deterministic matching/application. Election, reconnect, bounded queue, and per-RPC TCP connection setup can increase latency further. The implementation intentionally does not batch or acknowledge optimistically.

Request identity is `(client_id, request_id)` and the canonical fingerprint is the encoded protocol command. A conflicting payload under a previously committed key is `Duplicate`. The first committed execution returns its trade events and terminal order response. Exact retries return only the original terminal order response; trade events are deliberately not replayed. On a lost connection, the client may not know whether the command committed; retrying the exact same bytes safely resolves that ambiguity. Rejections are also deterministic committed outcomes when they are produced by the exchange state machine.

The query surface is intentionally limited: `TcpNode::local_state()` returns a node-local applied snapshot. It can lag the current leader and is not linearizable. There is no TCP query protocol or read-index/lease mechanism.

## Failure model and known limitations

Assume crash-stop nodes, a stable voter set, unique stable IDs, non-Byzantine peers, and messages that can be delayed, reordered, duplicated, or lost. Quorum means a strict majority of the configured fixed membership.

Not implemented: Raft snapshots/log compaction, membership changes, pre-vote, joint consensus, peer authentication/TLS, linearizable reads, adaptive transport flow control, exactly-once external side effects, and formal/model-checked verification. Treat peer networks as trusted. Storage or state-machine failures latch the replicated service unavailable; restart and recovery are required, and the node does not continue processing Raft or successful writes. This integration is experimental and requires broader adversarial testing and independent review before production use.

## Fault-injection coverage and observed behavior

The test suite provides bounded evidence for selected cases, not a general fault-tolerance guarantee:

| Scenario | Test coverage | Observed/expected behavior | Limit |
|---|---|---|---|
| Kill leader, elect another, restart old leader | `atlas_replicated_node_tests` and `atlas_raft_fault_tests` | Remaining majority elects a higher-term leader; restarted node begins as follower, receives the higher term, repairs its divergent uncommitted suffix, and applies the committed prefix. | Process tests exercise local loopback and selected schedules only. |
| Isolate a node / no quorum | Both integration suites | Isolated node cannot advance commit/application independently; an actual TCP write receives no false `Accepted` response. No response during the bounded observation window is expected temporary unavailability. | Does not exhaustively prove all partition schedules. |
| Delay, drop, duplicate, reorder | `atlas_raft_fault_tests` | Fixed seed `0xA71A5FA017`; deterministic fake clock; seeded queue scheduling and bounded tick/delivery loops. Safety assertions compare every common committed log entry. | Seeded traces are repeatable examples, not exhaustive exploration. |
| Divergent uncommitted suffix and restart | `atlas_raft_fault_tests` plus deterministic Raft conflict tests | Uncommitted divergent entries are replaced after reconnection; mutually committed entries are asserted equal. | Only tested for selected log shapes and traces. |
| Crash during Raft log / snapshot publication | `atlas_raft_fault_tests`; snapshot recovery tests | Test hooks terminate child processes after temporary bytes are written but before atomic rename. Recovery retains the prior complete Raft log or replays the durable WAL when no snapshot was published. | Hooks test process termination at controlled publication boundaries, not device power loss or every instruction/syscall. |
| Retry, timeout, duplicate request IDs | `atlas_replicated_node_tests` | Exact retry resolves uncertain client delivery and returns the durable terminal response without replaying trades; conflicting bytes under a committed key are rejected. | Does not claim exactly-once external side effects. |
| Compare committed history and matching state | Both replication integration tests | Tests compare persisted committed prefixes and deterministic book state after catch-up/restart. | State comparison covers tested commands only. |
| Success only after commit and application | Raft fault test plus no-quorum TCP test | Success precondition requires leader commit and apply plus a majority at the tested point; isolated TCP leader does not send `Accepted`. | Not a model check of all handler interleavings. |
| Snapshot installation and Raft log compaction | **Not implemented; not tested.** | The current Raft protocol has no InstallSnapshot RPC or compacted-log metadata. | Do not infer recovery after Raft snapshot installation/compaction. The separate order-book WAL snapshot is not a Raft snapshot. |

The randomized scheduler prints the fixed seed on both success and failure. CTest applies 45-second timeouts to network/process fault suites; simulated protocol runs also have fixed maximum tick and per-tick delivery counts. `atlas_raft_fault_tests` explicitly reports snapshot installation/log compaction as unsupported rather than a passing guarantee.
