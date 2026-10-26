# Atlas Project Explained

This project is an exchange-style matching engine and distributed trading system written in C++20. At a high level, it simulates an order book, accepts client orders, matches buy and sell orders deterministically, and can optionally run in a replicated three-node Raft configuration for durability and fault tolerance.

The project is documented in [README.md](README.md), [docs/domain-models.md](docs/domain-models.md), [docs/order-book-spec.md](docs/order-book-spec.md), and [docs/raft-design.md](docs/raft-design.md).

---

## 1. What this project does

Atlas is not just a toy demo. It contains several layers:

- A deterministic matching engine for limit orders
- A protocol layer for encoding and decoding wire messages
- A TCP server and client for local exchange interaction
- A write-ahead log (WAL) for durable local command storage
- Snapshot recovery for restart and replay
- An experimental three-node replicated mode using Raft
- Benchmark infrastructure for performance evaluation

In practical terms, the system accepts orders such as “buy 5 units at price 105” or “sell 4 units at price 101”, applies them to an order book, and emits trades when price and side conditions are met.

---

## 2. Core behavior

### Matching engine

The main matching logic is a single-instrument order book:

- Best bid = highest buy price
- Best ask = lowest sell price
- Matching is price-time priority
- Same-price orders are matched in FIFO order
- Incoming limit orders match resting orders at the maker’s price
- Partial fills are supported
- Residual quantity remains resting if not fully filled
- Market orders are not supported in this milestone

This means the engine is deterministic: given the same initial state and same sequence of commands, the same trades, states, and final book are produced.

### Order lifecycle

An order can be:

- Accepted and live
- Partially filled
- Fully filled
- Canceled

Important rules:

- Order IDs are unique for the lifetime of the book
- Request IDs are unique per client for that lifetime
- Same-client self-trading is rejected before mutation
- Duplicate requests for the same client/request pair are rejected
- Overflow and invalid numeric values are rejected instead of wrapped

### Trade rules

- The trade price is always the resting order’s price
- A trade is only created when prices cross
- Trade quantity is the minimum of taker leaves and maker leaves
- Both sides are updated consistently

---

## 3. Features

### 3.1 Deterministic order matching

The engine is designed to avoid randomness and wall-clock dependence. Matching behavior depends only on the ordered command stream and the book state.

Features include:

- Price-time priority
- FIFO behavior within price levels
- Partial fill support
- Cancellation of live orders
- Validation before mutation
- Sequence tracking for accepted commands and trades

### 3.2 Protocol and wire encoding

The project has a standalone protocol layer in the `atlas_protocol` library.

It defines message frames with:

- frame length
- version
- message type
- payload

Supported message types include:

- submit order
- cancel order
- order response
- trade event
- heartbeat

The protocol is strict and versioned. It rejects malformed, oversized, or unexpected frames instead of guessing.

### 3.3 TCP node and client

The `atlas-node` binary runs a TCP server that accepts trading protocol messages. The `atlas-client` binary sends requests to it.

Typical flow:

1. Start the node
2. Send a submit or cancel request
3. The server validates and applies the command
4. Matching engine executes it
5. Response is returned to the client
6. Trade events are emitted for the client that submitted the taker command

This is a single-node exchange node, not a public exchange broadcast system.

### 3.4 Write-ahead log and recovery

Atlas includes a local durable WAL. It records deterministic, replayable engine commands such as:

- valid submit commands
- valid cancel commands

The WAL is designed for crash recovery and deterministic replay. It validates:

- format/version
- record length
- checksum
- command ordering
- malformed inputs

Important durability modes:

- `Buffered`: OS accepts the write; no strong crash guarantee
- `Durable`: write followed by `fsync()`, giving stronger local durability guarantees

The system also supports snapshot creation and restart recovery so the engine can rebuild state from durable log history.

### 3.5 Replicated three-node mode

Atlas can run in a fixed three-node Raft configuration.

This mode adds:

- leader/follower election
- replicated log entry acceptance
- majority commit checks
- deterministic client outcome tracking
- durable request identities
- state rebuild from committed log replay

The node receives requests over the same TCP listener, but client responses are only acknowledged after the command is committed by a majority and applied locally.

This is experimental and intentionally conservative.

### 3.6 Benchmarking

There are benchmark modes for:

- matching
- gateway
- WAL
- Raft
- replicated
- snapshot
- mixed

The benchmark framework records throughput, latency, resource use, and failure data under `benchmarks/results/`.

---

## 4. How the system works end-to-end

### Single-node mode

1. Client sends a request using the protocol
2. Node validates the request
3. Matching engine checks crossing conditions and same-client restrictions
4. Matching performs trades against best opposite-side orders
5. Remaining quantity rests in the book if needed
6. The result is returned in an `OrderResponse`
7. The WAL stores accepted deterministic commands for replay

### Replicated mode

1. A client sends a write request to a node
2. If the node is not leader, it replies with `NotLeader`
3. Leader records the request in the Raft log
4. Majority of nodes replicate and commit it
5. The leader applies the command to the local order book
6. The terminal result is returned to the client
7. A retry of the same request gets the original recorded response instead of replaying trades

---

## 5. Important notes and caveats

### 5.1 This is not a production-grade exchange

Although the project is full-featured for a research/demo system, it explicitly notes the following limitations:

- No authentication or TLS
- No membership changes in Raft
- No Raft snapshot/log compaction
- No linearizable reads for local state queries
- No market data broadcast
- No query protocol for reading live exchange state from clients
- No formal safety proof for the replicated system
- File durability is local-fs based, not universal power-loss protection

### 5.2 Consensus is intentionally conservative

In replicated mode, client writes are acknowledged only after commit and local application. This means:

- more latency than a local in-memory engine
- possible retry logic for followers or timeouts
- a client must be prepared for `NotLeader`
- same request can be retried safely using the same `(client_id, request_id)` pair

### 5.3 WAL and snapshot guarantees are local, not magical

The WAL durability model is stronger than a plain buffered append, but it is still limited to the local filesystem assumptions.

`fsync()` improves durability against crashes and OS cache loss, but it does not promise immunity to all hardware failures, damaged controllers, or distributed storage problems.

### 5.4 Market orders are intentionally disabled

The system currently supports limit orders, not market orders. This is an explicit design choice. Market order support is deferred because it requires additional execution-price protections.

### 5.5 Determinism is a design goal

The matching engine is intended to be deterministic and repeatable. This is important for:

- testing
- replay recovery
- Raft log application verification
- benchmarking fairness

---

## 6. How to start and test the project

### 6.1 Build the project

From the repo root:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

This compiles the matching engine, protocol layer, node server, client, CLI, benchmark binary, and the test executables.

### 6.2 Smoke test

Check the simplest startup path:

```bash
./build/atlas
```

Expected output is:

```text
Project Atlas starting
```

### 6.3 Run the full automated test suite

The repo uses CTest:

```bash
ctest --test-dir build --output-on-failure
```

This runs the project’s startup smoke check and the matching, protocol, WAL, snapshot, node, Raft, and fault-injection tests.

### 6.4 Run a single test binary directly

You can also run an individual test target without CTest:

```bash
./build/atlas_matching_tests
./build/atlas_domain_tests
./build/atlas_wal_tests
./build/atlas_snapshot_tests
./build/atlas_protocol_tests
./build/atlas_node_tests
./build/atlas_raft_tests
./build/atlas_raft_fault_tests
./build/atlas_replicated_node_tests
```

### 6.5 Run the interactive CLI demo

This is the easiest way to visually verify the matching engine:

```bash
./build/atlas_cli
```

Or run the included demo script:

```bash
./build/atlas_cli < scripts/atlas-demo.txt
```

To make the output cleaner and non-interactive:

```bash
./build/atlas_cli --no-prompt < scripts/atlas-demo.txt
```

### 6.6 Run the single-node TCP exchange

Start the server in one terminal:

```bash
./build/atlas-node --bind 127.0.0.1 --port 9000
```

Then send an order from another terminal:

```bash
./build/atlas-client 127.0.0.1 9000 101 submit 1 1001 1 SELL 105 5
```

You can also cancel an order using the client format described in the project docs.

### 6.7 Run the replicated three-node setup

Use separate storage directories and three distinct node IDs:

```bash
./build/atlas-node --replicated --node-id 1 --bind 127.0.0.1 --port 9001 \
  --storage-dir data/node1 --peer 2,127.0.0.1,9002 --peer 3,127.0.0.1,9003

./build/atlas-node --replicated --node-id 2 --bind 127.0.0.1 --port 9002 \
  --storage-dir data/node2 --peer 1,127.0.0.1,9001 --peer 3,127.0.0.1,9003

./build/atlas-node --replicated --node-id 3 --bind 127.0.0.1 --port 9003 \
  --storage-dir data/node3 --peer 1,127.0.0.1,9001 --peer 2,127.0.0.1,9002
```

This is used for Raft-based distributed behavior and should be treated as experimental.

### 6.8 Run benchmarks

The benchmark runner is:

```bash
./scripts/run_atlas_perf.sh
```

For a specific benchmark mode:

```bash
python3 scripts/atlas_perf.py --mode MATCHING
```

Check the benchmark docs in [benchmarks/METHODOLOGY.md](benchmarks/METHODOLOGY.md) for more details.

---

## 7. Recommended testing order

For a clean validation workflow, use this order:

1. Build the project
2. Run the smoke test
3. Run `ctest --output-on-failure`
4. Try the interactive CLI demo
5. Start the single-node TCP server and send a real order
6. If needed, test the replicated 3-node setup
7. Run benchmarks only after the core tests pass

This gives you the easiest path from “build works” to “core exchange behavior works” to “distributed mode works.”

---

## 8. Typical commands

### Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

### Run a single-node demo

```bash
./build/atlas-node --bind 127.0.0.1 --port 9000
./build/atlas-client 127.0.0.1 9000 101 submit 1 1001 1 SELL 105 5
```

### Run replicated mode

Each node uses its own storage directory and a fixed three-node membership.

```bash
./build/atlas-node --replicated --node-id 1 --bind 127.0.0.1 --port 9001 \
  --storage-dir data/node1 --peer 2,127.0.0.1,9002 --peer 3,127.0.0.1,9003

./build/atlas-node --replicated --node-id 2 --bind 127.0.0.1 --port 9002 \
  --storage-dir data/node2 --peer 1,127.0.0.1,9001 --peer 3,127.0.0.1,9003

./build/atlas-node --replicated --node-id 3 --bind 127.0.0.1 --port 9003 \
  --storage-dir data/node3 --peer 1,127.0.0.1,9001 --peer 2,127.0.0.1,9002
```

### Interactive demo

```bash
./build/atlas_cli
```

Commands include:

- `NEW BUY ...`
- `NEW SELL ...`
- `CANCEL ...`
- `SHOW BOOK`
- `SHOW TRADES`
- `HELP`
- `QUIT`

---

## 7. Summary

Atlas is best understood as a layered C++ trading system:

- matching engine for deterministic order execution
- strict protocol for client/server communication
- WAL and snapshot recovery for local durability
- Raft replication for consensus across three nodes
- benchmark harness for measuring performance and reliability

The project’s core strength is the combination of deterministic matching logic and a careful, explicit replication and recovery model. Its biggest caution is that the distributed/replicated components are still experimental and should be treated as an engineering prototype rather than a production-grade trading platform.

---

## 8. Recommended mental model

If you are walking through the codebase, think of it in these layers:

1. Domain values and order semantics
2. Matching engine and order book
3. Protocol framing and message handling
4. TCP node and client interaction
5. WAL/snapshot durability and recovery
6. Raft replication state machine
7. Benchmarking and validation

That structure makes the project easier to understand than reading it as a single monolith.
