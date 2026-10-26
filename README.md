# Project Atlas

Project Atlas is a C++20 stock-exchange simulator project. It includes a deterministic price-time matching engine, versioned TCP protocol, and an optional fixed three-node Raft mode that replicates submit/cancel commands and applies committed entries to each node's exchange state machine. The replicated mode is conservative and experimental: client writes are acknowledged only after commit and local application, and local snapshots are not linearizable reads. See [docs/domain-models.md](docs/domain-models.md), [docs/raft-design.md](docs/raft-design.md), and the limitations below.

## Prerequisites

- Ubuntu 24.04 LTS (or another compatible Linux environment)
- GCC 13.3 or another C++20-capable compiler
- CMake 3.28 or newer
- Git

## Configure and build

From the project root:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

## Test

```sh
ctest --test-dir build --output-on-failure
```

CTest runs the startup smoke test, matching and domain validation tests, WAL/snapshot recovery tests, protocol tests, TCP loopback tests, deterministic Raft tests, a seeded message-fault harness, and three-node TCP replication/recovery integration tests. The matching tests cover validation, matching priority, partial fills, cancellation, self-trade prevention, sequence ordering, quantity boundaries, and deterministic generated order streams.

## Matching rules

- Best price first: highest bid and lowest ask.
- FIFO order among orders at the same price.
- Incoming limit orders match eligible resting orders; each trade executes at the resting order's price.
- Partial fills update both orders; unfilled incoming limit quantity rests at its limit price.
- Market orders are not supported. Same-client self-trading is rejected atomically.
- IDs remain reserved for the lifetime of the `OrderBook`, including after fill or cancellation. Client request IDs are unique per client for that lifetime.
- Prices are positive signed integer ticks, quantities are positive unsigned integer units, and overflow is rejected rather than wrapped.

Example: if a sell limit for 4 units rests at 101 ticks, then a buy limit for 3 units at 105 ticks trades 3 units at 101 ticks. The sell has 1 unit remaining; the buy is fully filled.

```sh
./build/atlas
```

Expected output:

```text
Project Atlas starting
```

## Write-ahead log (WAL)

Project Atlas includes a durable WAL module that records only deterministic, replayable engine commands. The WAL is intentionally separate from the matching engine: the engine accepts a submit or cancel command, and the storage layer serializes that command as an append-only record. The exact commands recorded are:

- `submit(OrderRequest)` for a valid limit order accepted by the engine
- `cancel(OrderId)` for a live order cancellation accepted by the engine

The record format is explicitly versioned and length-delimited, not a raw C++ struct dump. Each record is encoded as:

```text
[version:1][type:1][command_sequence:8][payload_length:4][payload][checksum:8]
```

where `payload` is a compact binary encoding of the command arguments.

Durability modes:

- `Buffered`: write succeeds when the OS accepts the append; the data is not guaranteed to survive an OS crash or power loss until the file is flushed to stable storage.
- `Durable`: the append is followed by `fsync()`. Acknowledgement is returned only after the write and `fsync()` succeed. This is the mode that provides the strongest local durability guarantee available from the kernel without adding a more complex storage stack.

`fsync()` is not a magical guarantee of power-failure survival in every environment; it ensures the kernel flushes the file data and metadata from page cache to storage. It does not make a remote, replicated, or RAID-protected storage system magically durable across all failure modes. It is necessary for durable local file updates, but not sufficient for distributed durability guarantees.

The WAL rejects truncated records, malformed lengths, checksum mismatches, unknown versions/types, and command-sequence mismatches. Replay into a fresh `OrderBook` is deterministic: the same sequence of accepted commands re-creates the same order state and trade stream.

## Snapshots and restart recovery

The single-node recovery layer checkpoints an `OrderBook` together with its WAL byte offset. Snapshot version 1 uses an explicit little-endian encoding with magic, version, payload length, WAL offset, engine sequence counters, instrument identity, all accepted order states (including filled/canceled orders so IDs and client request IDs remain reserved), and a trailing FNV-1a 64-bit integrity checksum. Live orders are restored in engine-command order, preserving FIFO priority. The decoder checks the file and payload lengths, version, checksum, enum/domain values, duplicate identifiers, sequence counters, and book invariants before changing the target book.

`SnapshotStore::save()` is a quiescent single-threaded checkpoint operation: callers must stop submitting commands while it runs and keep the engine state aligned with the WAL. It synchronizes the WAL, replays it to verify that the supplied engine state exactly matches the durable command history, then records the resulting byte offset. It writes a uniquely named temporary file in the snapshot directory, `fsync()`s and closes it, atomically renames it over the published snapshot, and `fsync()`s the containing directory. Recovery loads that snapshot and replays only complete WAL records after its byte offset. If there is no published snapshot, recovery replays from the beginning; an existing invalid snapshot or a snapshot pointing past the WAL is an error, not a reason to silently start empty.

For acknowledged-command retention, use `WalDurabilityMode::Durable`: an append is acknowledged only after the record write and WAL `fsync()` succeed. `Buffered` acknowledgement means only that the OS accepted the write; it is not covered by the no-loss-on-process/power-failure statement. Snapshot publication is also synchronized, but it does not replace the WAL or claim replicated durability. Recovery assumes the WAL has not been truncated or replaced independently of the snapshot.

The recovery suite includes a forked process-termination test that exits immediately after a durable WAL acknowledgement and verifies recovery in the parent process. This tests abrupt process exit while the kernel and storage device remain running. It is **not** a power-loss test: it does not simulate loss of kernel page cache, device write caches, filesystem/controller failures, or faulty `fsync()` implementations. The implementation makes the local `fsync()`/atomic-rename durability request and does not claim universal power-loss safety or any distributed guarantee. A real power-loss validation requires dedicated hardware/filesystem fault testing.

## Client/server wire protocol

The `atlas_protocol` library is an engine- and socket-independent C++20 codec in [include/atlas/protocol.hpp](include/atlas/protocol.hpp). It never serializes C++ object memory. All integer fields use unsigned big-endian/network byte order; signed 64-bit prices use their two's-complement bit pattern in the same byte order. Enum values are explicit protocol numbers.

Each frame is:

```text
[frame_length:u32 BE][version:u8][message_type:u8][payload]
```

`frame_length` counts version, type, and payload bytes but excludes its own four-byte prefix. Version 1 permits at most 4096 frame bytes. Message type and version are validated before interpreting payloads; each type has an exact payload length, and IDs, quantities, prices, enums, and response-state combinations are range-checked.

Version 1 message payloads, in field order:

| Type | Fields |
|---|---|
| Submit order | client ID, request ID, order ID, instrument ID (`u64` each); side (`u8`); order type (`u8`); limit price (`i64`); quantity (`u64`) |
| Cancel order | client ID, request ID, order ID (`u64` each) |
| Order response | client ID, request ID, order ID, command sequence (`u64` each); status (`u8`); result code (`u16`); filled, leaves, canceled quantities (`u64` each) |
| Trade event | trade sequence, command sequence, instrument ID, maker/taker order IDs, maker/taker client IDs (`u64` each); taker side (`u8`); price (`i64`); quantity (`u64`) |
| Heartbeat | client ID, request ID, nonce (`u64` each); ping/pong kind (`u8`) |

The `OrderResponse` result codes include `Accepted`, `Invalid`, `Duplicate`, `NotFound`, `NotLive`, `InternalError`, and `NotLeader`. In replicated mode, `NotLeader` means the node did not append the write; retry the same request against another configured node.

The client ID plus request ID identifies a request for caller-managed deduplication; the codec validates those IDs but does not persist deduplication state or assign server semantics. `read_message()` and `write_message()` accept transport callbacks and loop until the complete prefix/body is read or frame is written, handling short transfers without depending on TCP or including a socket implementation. EOF, callback errors, no-progress writes, oversize lengths, and malformed frames fail with `ProtocolError`.

Compatibility policy: version 1 is strict. Receivers reject unknown versions, types, enum values, malformed lengths, and trailing bytes rather than guessing. Incompatible framing or semantic changes require a new protocol version. Compatible extensions require an explicitly designed versioned extension mechanism; unknown-field skipping and version negotiation are not implemented.

## TCP node and client

`atlas-node` is the POSIX IPv4 TCP server and `atlas-client` is a one-request command-line client. For a local demo, start the server in one terminal:

```sh
./build/atlas-node --bind 127.0.0.1 --port 9000
```

Then submit an order from another terminal:

```sh
./build/atlas-client 127.0.0.1 9000 101 submit 1 1001 1 SELL 105 5
```

The client prints an `ORDER_RESPONSE`; when an incoming order matches, it prints the associated `TRADE` event before that response. Cancellation uses `./build/atlas-client 127.0.0.1 9000 CLIENT_ID cancel REQUEST_ID ORDER_ID`. Stop the node with Ctrl-C or SIGTERM.

Ownership is explicit: the accept loop creates bounded per-connection I/O workers; those workers decode frames and enqueue typed commands into a bounded command queue, but never access the `OrderBook`. Exactly one execution thread owns and mutates the matching engine and in-memory request-deduplication table. Each I/O worker waits for at most one command result at a time, so the outstanding response handoff is bounded by the configured connection count; socket send/receive timeouts close stalled peers. Trade events are returned only on the connection submitting the taker command; market-data broadcast is not implemented. Defaults are 64 connections, a 256-command queue, 100,000 retained request keys, and 100,000 accepted order records. Configure the connection and queue limits with `--max-connections` and `--queue-capacity`; protocol frames remain limited to 4096 bytes. Excess connections are closed and commands received while the queue is full get a rejected `InternalError` response.

In single-node mode, deduplication is in-memory. In replicated mode, see the following section for durable request identity, recovery, and acknowledgement behavior.

## Three-node replicated exchange mode

Start each process with its own node ID, TCP endpoint, and **unique persistent storage directory**. Each `--peer` is another node's Raft/client TCP endpoint. The same listener accepts client protocol frames and Raft envelopes.

```sh
./build/atlas-node --replicated --node-id 1 --bind 127.0.0.1 --port 9001 \
	--storage-dir data/node1 --peer 2,127.0.0.1,9002 --peer 3,127.0.0.1,9003
./build/atlas-node --replicated --node-id 2 --bind 127.0.0.1 --port 9002 \
	--storage-dir data/node2 --peer 1,127.0.0.1,9001 --peer 3,127.0.0.1,9003
./build/atlas-node --replicated --node-id 3 --bind 127.0.0.1 --port 9003 \
	--storage-dir data/node3 --peer 1,127.0.0.1,9001 --peer 2,127.0.0.1,9002
```

The membership is fixed at exactly three distinct IDs and peer endpoints; membership changes are not supported. Use election timeouts safely above the heartbeat interval and allow an election to complete before submitting. A request sent to a follower/candidate receives `NotLeader`; clients should retry the same `(client_id, request_id)` against another configured endpoint. After losing leadership, the former leader fails outstanding waiters without claiming success. A disconnected client may not receive a response even if the command later commits; retrying the same exact request returns the recorded result, while reuse of that key with different request bytes is rejected as `Duplicate`.

The original request receives its deterministic trade events (if any) and terminal order response **only after the log entry is committed by a majority and applied locally**. An exact retry receives the original terminal order response only; trade events are not replayed, so clients do not process the same trade twice. Committed commands and request fingerprints are durable in each node's Raft store. Startup rebuilds the matching book by replaying the durable committed prefix in order and verifies durable outcomes against deterministic execution. Each process must use its own directory; do not share a node's storage directory with another process.

Replication adds consensus latency: before a successful response, the leader durably writes the command, receives a replication acknowledgement from at least one of the two followers (a majority of three), durably records commit progress, and applies the command. Expect at least a leader-to-follower network round trip plus storage synchronization and matching work; elections, reconnects, queueing, and per-RPC TCP connection setup can add further delay. This initial path intentionally favors an understandable quorum-before-acknowledgement rule over batching or low latency.

No client query protocol is currently provided. `TcpNode::local_state()` is an in-process snapshot of that node's applied prefix and may lag the leader; it is explicitly **not linearizable**. The fixed-membership Raft core, file store, and integration are experimental and have not had formal verification or production security review. They assume non-Byzantine trusted peers, stable unique IDs, and reliable local filesystem `fsync` semantics; there is no authentication, TLS, membership change, log compaction/snapshotting for Raft, or protection against arbitrary storage/device failure. See [docs/raft-design.md](docs/raft-design.md) for the detailed execution and recovery contract.
## Performance evaluation

The benchmark framework has separate `matching`, `gateway`, `wal`, `raft`, `replicated`, `snapshot`, and `mixed` modes. See [benchmarks/METHODOLOGY.md](benchmarks/METHODOLOGY.md) for measurement semantics, open/closed-loop methodology, durability guarantees, sample thresholds, and limitations.

The reproducible measured report is [benchmarks/results/release-performance-20261025/REPORT.md](benchmarks/results/release-performance-20261025/REPORT.md). It includes the VM/build context, five valid trials per mode/generator, throughput and latency distributions, resource measurements, rejects/timeouts, and limitations. Its results distinguish in-memory matching and controlled Raft from local durable WAL and three-node committed-and-applied TCP service.

Build and run a complete benchmark campaign (tests run before measurements):

```sh
./scripts/run_atlas_perf.sh
```

The campaign writes JSONL trial metadata, raw per-operation samples, and a CSV analysis summary under `benchmarks/results/`. The measured example is [benchmarks/results/verified-performance-20261025/REPORT.md](benchmarks/results/verified-performance-20261025/REPORT.md), with raw samples and JSONL adjacent. For an individual mode, use `scripts/atlas_perf.py --mode MODE`; `--generator open-loop` is supported only for the gateway and replicated TCP modes. Set `TRIALS`, `CLIENTS`, `RATE`, `GENERATOR`, `VM`, and `SEED` to control and record a campaign. `OPS` overrides the operation count for all modes; `MATCHING_OPS`, `WAL_OPS`, `RAFT_OPS`, `SNAPSHOT_OPS`, `MIXED_OPS`, `GATEWAY_OPS`, and `REPLICATED_OPS` override counts individually. Raw measurements are never manufactured when a mode fails.

These are local development measurements. In particular, **VirtualBox results are virtualised measurements, not production HFT exchange performance**. Controlled in-memory Raft timings do not have the same network/storage guarantees as the three-node replicated mode; compare only with those distinctions explicit.

## Interactive matching demo

Build and start the separate CLI:

```sh
./build/atlas_cli
```

Commands are `NEW BUY <order_id> <client_id> <request_id> <price_ticks> <quantity>`, `NEW SELL` with the same fields, `CANCEL <order_id>`, `SHOW BOOK`, `SHOW TRADES`, `HELP`, and `QUIT`. This demonstration uses instrument ID 1. IDs and numeric inputs must be positive integers. Unsupported or malformed input is reported and does not crash the CLI.

To replay the deterministic demo session:

```sh
./build/atlas_cli < scripts/atlas-demo.txt
```

The script rests a sell for 4 units at 101 ticks, submits a buy for 3 units at 105 ticks, displays the one-unit sell remainder and trade, then cancels the remainder and shows the final empty book. Use `./build/atlas_cli --no-prompt < scripts/atlas-demo.txt` for a clean, non-interactive transcript. CLI input is whitespace-separated, uppercase, and one command per line; malformed lines are reported and ignored. Lines whose first token begins with `#` are comments.
