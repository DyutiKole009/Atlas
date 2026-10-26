# Project Atlas performance evaluation methodology

## Scope and guarantees

The suite measures seven distinct paths and does not equate their guarantees:

| Mode | What it measures | Durability / completion point |
|---|---|---|
| `matching` | `OrderBook::submit` matching execution | In-memory only; no socket or durable write |
| `gateway` | TCP framing, client/server handling, queueing, matching, response | Single node; in-memory order book and request deduplication |
| `wal` | Matching plus one durable WAL append for each accepted submit | WAL append `fsync` per accepted command |
| `raft` | Controlled three-node in-memory Raft RPC/commit/application path, no TCP or durable disk I/O | The leader and quorum receive simulated messages; leader command is committed/applied |
| `replicated` | TCP client path through three independent node processes | Reports success only for `Accepted`; in the service this means majority commit and local application, with per-node persistent file updates |
| `snapshot` | Durable WAL workload, atomic snapshot creation and full recovery | WAL/snapshot `fsync`, rename, directory sync, then recovery correctness check |
| `mixed` | Deterministic submit/cancel pattern in the matcher | In-memory; 3 submits for each cancellation attempt |

`gateway` and `replicated` are end-to-end client latencies. `raft` is controlled protocol overhead and must not be compared as a substitute for replicated TCP latency. Standalone and replicated measurements have different durability and acknowledgement semantics; any comparison must state that explicitly.

## Environment and repeatability
Build the chosen configuration before measuring; record compiler, compiler version, CMake build type, kernel/platform, CPU count, VM product name, node/client count, seed, workload distribution, operation count, rate, timeout, and durability. Supply `--vm` when DMI does not describe the VirtualBox guest accurately. Each trial uses a recorded seed; trial $i$ uses `base_seed + i`. Use a fresh output directory per campaign and preserve both raw per-operation CSV and JSONL run metadata.

The scripts use only Python's standard library. `atlas_perf.py` launches a local single node for `gateway` and a private three-process cluster with separate temporary storage directories for `replicated`, unless `--external-gateway` is provided for standalone gateway benchmarking. They shut down processes with a bounded wait and kill on timeout. Network modes use loopback by default. No timestamps from different machines are subtracted: latency is elapsed `time.monotonic_ns()` measured at the client process.

The C++ microbenchmarks use `std::chrono::steady_clock`; each raw CSV row retains the operation sample, outcome, operation type, seed, total elapsed time, accepted/rejected/error counts, compiler ID/version, build type, topology, workload description, CPU times, and peak RSS. The Python JSONL metadata records VM name from the Linux DMI product name where available; use `--vm` to supply the VirtualBox guest configuration (vCPU/RAM) explicitly. Python derives throughput from accepted operation count divided by elapsed test time, latency percentiles from successful operation samples, and runner CPU/memory from process resource counters. For local gateway/replicated runs, Linux `/proc` sampling estimates aggregate Atlas node CPU and peak resident set size. It samples every 10 ms, so short-lived peaks can be missed. CPU utilization sums process CPU across cores and therefore can exceed 100%; CPU/resource values for external nodes are not included. `ru_maxrss` reports the benchmark runner or child maximum resident set size (KiB on Linux); verify platform units before comparing systems.

## Load generation and latency methodology

Closed-loop mode has up to `--clients` clients; each waits for its response before sending its next order. This is useful for interactive response-time behavior but limits offered load when requests slow down.

Open-loop mode is available for gateway and replicated modes. Target arrival times are fixed at start plus $i / rate$ and submissions use a bounded client worker pool with separate TCP connections. Latency begins at the scheduled arrival timestamp—not when the generator eventually dispatches the work—so scheduler lateness is counted and coordinated omission is avoided as far as this runner permits. Replicated open-loop retries the identical request after `NotLeader`, using new connections and configured endpoints; retries are included in that request's elapsed latency. The reported end-to-end latency percentiles use successfully accepted responses only; report rejects, timeouts, errors, and sample counts alongside them. Successful latency P50 is reported with at least one sample, P95 only at 20+, P99 only at 100+, and P99.9 only at 1,000+ samples. Percentiles use a deterministic lower empirical rank, not an interpolated estimate.

C++ modes are synchronous serial workloads (closed-loop); the CLI rejects open-loop for these modes. WAL mode records both matching-submit and WAL append/fsync samples; report the stage separately and use logical submit count for overall throughput. Snapshot mode likewise reports snapshot-create and snapshot-recover operations separately; only five create/recovery observations are available in the standard 5-trial campaign, so no tail percentiles are defensible. Socket timings include client frame construction/send and waiting for a decoded order response. Gateway C++ codec execution on client and server is included, not isolated as a codec-only microbenchmark. Network modes do not estimate one-way latency.

## Failures, repetitions, and variability

Each trial has connection and startup timeouts; C++ subprocess runs have a 60-second minimum hard ceiling. CTest's functional suite remains independent from benchmark data collection. Failures, rejects, and timeouts are recorded separately from successful latency samples. An unavailable cluster is not reported as a successful zero-latency result.

Run at least 5 independent trials for exploratory comparisons and preferably 10+ for stable estimates. Seeds change deterministically across trials, and the analysis script reports mean/stdev throughput plus median trial percentiles. Preserve individual trials rather than only an aggregate. Keep workload size, rate, client count, affinity, VM memory/vCPU configuration, background load, power mode, and filesystem/storage settings fixed between compared runs. Warm-up should be performed separately and recorded; do not silently discard samples.

No optimization should be made from a benchmark alone. Profile representative workloads first, formulate a hypothesis, change one factor, run correctness tests, and compare repeated raw trials. Correctness and durability guarantees are invariants, not performance knobs.

## Limitations and interpretation

The built-in cluster and gateway runs use localhost and a single host's storage. They do not emulate real exchange network topology, NIC/interrupt behavior, cross-host persistence, exchange colocations, market data fanout, production queues, kernel-bypass networking, or exchange-grade hardware. VirtualBox measurements are **virtualized development measurements, not production HFT exchange performance**. No benchmark run here is evidence of production suitability, HFT latency, formal consensus correctness, or power-loss survival. Raw measured results belong only to the exact recorded machine/build/configuration.
