#!/usr/bin/env python3
"""Reproducible Project Atlas performance suite. Python standard library only."""
import argparse
import csv
import json
import os
import platform
import resource
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BIN = ROOT / "build"
MODES = ("matching", "gateway", "wal", "raft", "replicated", "snapshot", "mixed")


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, int((p / 100.0) * (len(ordered) - 1))))
    return ordered[rank]


def stats(samples_ns):
    return {"p50_ns": percentile(samples_ns, 50) if len(samples_ns) >= 1 else None,
            "p95_ns": percentile(samples_ns, 95) if len(samples_ns) >= 20 else None,
            "p99_ns": percentile(samples_ns, 99) if len(samples_ns) >= 100 else None,
            "p999_ns": percentile(samples_ns, 99.9) if len(samples_ns) >= 1000 else None,
            "mean_ns": statistics.fmean(samples_ns) if samples_ns else None}


def mix64(value):
    value &= (1 << 64) - 1
    value ^= value >> 30
    value = value * 0xBF58476D1CE4E5B9 & ((1 << 64) - 1)
    value ^= value >> 27
    value = value * 0x94D049BB133111EB & ((1 << 64) - 1)
    return value ^ (value >> 31)


def order_for(i, seed, client_count=64):
    mixed = mix64(seed + i * 0x9E3779B97F4A7C15)
    side = 1 if i % 2 == 0 else 2
    # Use disjoint price bands by side to create reproducible resting workload.
    price = (90 + ((mixed >> 16) % 21)) if side == 1 else (110 + ((mixed >> 16) % 21))
    return (i + 1, (i % client_count) + 1, i + 1, 1, side, 1, price, 1 + ((mixed >> 8) % 5))


def protocol_frame(order):
    import struct
    client, request, oid, instrument, side, typ, price, qty = order
    body = struct.pack(">BBQQQQBBqQ", 1, 1, client, request, oid, instrument, side, typ, price, qty)
    return struct.pack(">I", len(body)) + body


def parse_order_response(sock, request_id):
    import struct
    while True:
        prefix = read_exact(sock, 4)
        size = struct.unpack(">I", prefix)[0]
        frame = read_exact(sock, size)
        if len(frame) < 2:
            raise RuntimeError("truncated response frame")
        typ = frame[1]
        if typ == 3:
            # Response layout starts after version/type: 4*u64, status:u8, result:u16, 3*u64
            fields = struct.unpack(">QQQQBHQQQ", frame[2:])
            if fields[1] == request_id:
                return fields[5], fields[1]
        elif typ != 4:
            raise RuntimeError(f"unexpected protocol response type {typ}")


def read_order_response(sock):
    import struct
    while True:
        size = struct.unpack(">I", read_exact(sock, 4))[0]
        frame = read_exact(sock, size)
        if len(frame) < 2:
            raise RuntimeError("truncated response frame")
        if frame[1] == 3:
            fields = struct.unpack(">QQQQBHQQQ", frame[2:])
            return fields[5], fields[1]
        if frame[1] != 4:
            raise RuntimeError(f"unexpected server message type {frame[1]}")


def read_exact(sock, count):
    parts = bytearray()
    while len(parts) < count:
        chunk = sock.recv(count - len(parts))
        if not chunk:
            raise RuntimeError("peer closed before full response")
        parts.extend(chunk)
    return bytes(parts)


def connect_client(host, port, timeout):
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(timeout)
    return sock


class ProcessMonitor:
    """Best-effort Linux /proc sampling for node CPU and aggregate RSS."""
    def __init__(self, processes):
        self.pids = [p.pid for p in processes]
        self.hz = os.sysconf("SC_CLK_TCK")
        self.stop_event = threading.Event()
        self.samples = []
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _sample(self):
        total_ticks = total_rss = 0
        for pid in self.pids:
            try:
                stat_text = Path(f"/proc/{pid}/stat").read_text()
                tail = stat_text[stat_text.rfind(")") + 2:].split()
                total_ticks += int(tail[11]) + int(tail[12])
                for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        total_rss += int(line.split()[1])
                        break
            except (OSError, ValueError, IndexError):
                continue
        return time.monotonic(), total_ticks, total_rss

    def _run(self):
        while not self.stop_event.is_set():
            self.samples.append(self._sample())
            self.stop_event.wait(0.01)
        self.samples.append(self._sample())

    def finish(self):
        self.stop_event.set()
        self.thread.join(timeout=1)
        if len(self.samples) < 2:
            return {"node_cpu_utilization_pct": None, "node_peak_rss_kib": None}
        begin, end = self.samples[0], self.samples[-1]
        elapsed = end[0] - begin[0]
        cpu_seconds = max(0, end[1] - begin[1]) / self.hz
        return {"node_cpu_utilization_pct": 100 * cpu_seconds / elapsed if elapsed else None,
                "node_peak_rss_kib": max(sample[2] for sample in self.samples)}


def start_single_gateway(args):
    port = reserve_ports(1)[0]
    process = subprocess.Popen([str(Path(args.bin_dir) / "atlas-node"), "--bind", args.host,
                                "--port", str(port)], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + args.timeout * 2
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("single-node gateway exited during startup")
        try:
            with connect_client(args.host, port, 0.25):
                return port, process
        except OSError:
            time.sleep(0.02)
    process.terminate()
    process.wait(timeout=2)
    raise TimeoutError("single-node gateway startup timed out")


def stop_process(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def new_result(mode, args, metadata):
    return {"mode": mode, "status": "ok", "seed": args.seed,
            "operations_requested": args.operations, "client_count": args.clients,
            "node_count": 3 if mode == "replicated" else 1,
            "workload_distribution": args.distribution, "duration_target_s": args.duration,
            "generator": args.generator, "durability": "durable" if mode in ("wal", "replicated", "snapshot") else "none",
            "config": metadata, "sample_count": 0, "completed": 0, "errors": 0,
            "rejected": 0, "timeouts": 0, "throughput_ops_s": 0.0,
            "elapsed_s": 0.0, "latency": stats([]), "cpu_user_s": None,
            "cpu_system_s": None, "cpu_utilization_pct": None,
            "peak_rss_kib": None, "notes": []}


def finalize(result, latencies, completed, elapsed, started_usage, notes=None):
    result["sample_count"] = len(latencies)
    result["completed"] = completed
    result["elapsed_s"] = elapsed
    result["throughput_ops_s"] = completed / elapsed if elapsed > 0 else 0.0
    result["latency"] = stats(latencies)
    end_usage = resource.getrusage(resource.RUSAGE_SELF)
    result["cpu_user_s"] = end_usage.ru_utime - started_usage.ru_utime
    result["cpu_system_s"] = end_usage.ru_stime - started_usage.ru_stime
    result["cpu_utilization_pct"] = 100.0 * (result["cpu_user_s"] + result["cpu_system_s"]) / elapsed if elapsed else None
    result["peak_rss_kib"] = end_usage.ru_maxrss
    if notes:
        result["notes"].extend(notes)
    return result


def run_matching(args, meta):
    # The benchmark is an external harness. To avoid binding a second implementation,
    # execute the compiled C++ microbenchmark via the source-specific mode added below.
    return run_cli_benchmark(args, meta, "matching")


def run_cli_benchmark(args, meta, mode):
    result = new_result(mode, args, meta)
    output = Path(args.output_dir) / f"raw_{mode}_{args.seed}_{time.time_ns()}.csv"
    cmd = [str(Path(args.bin_dir) / "atlas-bench"), "--mode", mode,
           "--orders", str(args.operations), "--seed", str(args.seed),
           "--output", str(output)]
    try:
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=max(60.0, args.timeout))
    except subprocess.TimeoutExpired:
        result["status"] = "timeout"
        result["timeouts"] = 1
        result["errors"] = 1
        result["notes"].append("C++ benchmark process exceeded configured timeout")
        return result
    if proc.returncode != 0:
        result["status"] = "failed"
        result["errors"] += 1
        result["notes"].append(proc.stderr.strip() or proc.stdout.strip())
        return result
    with output.open(newline="") as handle:
        records = list(csv.DictReader(handle))
    if records:
        row = records[-1]
        component_rows = records
        transaction_rows = [item for item in component_rows if item["operation"] in
                            ("submit", "matching_submit", "cancel")]
        report_rows = transaction_rows if transaction_rows else component_rows
        # Component modes expose per-stage samples, but throughput/error accounting
        # uses the primary submitted command transactions, not multiple timing stages.
        row = report_rows[-1]
        successful = [item for item in records if item["success"] == "1"]
        completed = sum(item["success"] == "1" for item in report_rows)
        latency_rows = successful
        if mode == "wal":
            latency_rows = [item for item in successful if item["operation"] == "wal_append_fsync"]
        elif mode == "snapshot":
            latency_rows = [item for item in successful if item["operation"] in ("snapshot_create", "snapshot_recover")]
        latencies = [int(item["latency_ns"]) for item in latency_rows]
        operation_groups = {}
        for item in successful:
            operation_groups.setdefault(item["operation"], []).append(int(item["latency_ns"]))
        result["latency_by_operation"] = {name: stats(values) for name, values in operation_groups.items()}
        elapsed = float(row["elapsed_s"])
        result["completed"] = completed
        result["sample_count"] = len(records)
        result["rejected"] = int(row["rejected"])
        result["errors"] = int(row["errors"])
        result["elapsed_s"] = elapsed
        result["throughput_ops_s"] = completed / elapsed if elapsed else 0.0
        result["latency"] = stats(latencies)
        result["cpu_user_s"] = float(row["cpu_user_s"])
        result["cpu_system_s"] = float(row["cpu_system_s"])
        result["cpu_utilization_pct"] = 100 * (result["cpu_user_s"] + result["cpu_system_s"]) / elapsed if elapsed else None
        result["runner_cpu_utilization_pct"] = result["cpu_utilization_pct"]
        result["peak_rss_kib"] = int(row["peak_rss_kib"])
    result["raw_csv"] = str(output)
    raw_rows = []
    for row in records:
        raw_rows.append({"operation": row["operation"], "latency_ns": int(row["latency_ns"]),
                         "success": row["success"] == "1", "trades": int(row["trades"])})
    result["raw_samples_csv"] = write_sample_csv(args, mode, raw_rows)
    if records:
        for field, key in (("workload_distribution", "workload_distribution"),
                           ("durability", "durability"), ("client_count", "client_count"),
                           ("node_count", "node_count"), ("vm_configuration", "vm_configuration")):
            if row.get(key):
                result[field] = row[key]
    result["generator"] = "closed-loop"  # In-process microbenchmarks execute serially.
    if mode == "matching":
        result["durability"] = "in-memory only"
    elif mode == "wal":
        result["durability"] = "WAL fsync per accepted command"
    elif mode == "snapshot":
        result["durability"] = "WAL fsync per accepted command; atomic snapshot fsync/rename"
    elif mode == "raft":
        result["durability"] = "in-memory controlled Raft transport; no disk fsync"
    elif mode == "mixed":
        result["durability"] = "in-memory only; 3 submissions per cancellation attempt"
    result["notes"].append(proc.stdout.strip())
    return result


def run_wal(args, meta):
    return run_cli_benchmark(args, meta, "wal")


def run_snapshot(args, meta):
    return run_cli_benchmark(args, meta, "snapshot")


def run_raft(args, meta):
    return run_cli_benchmark(args, meta, "raft")


def run_mixed(args, meta):
    return run_cli_benchmark(args, meta, "mixed")


def run_gateway(args, meta, replicated=False):
    mode = "replicated" if replicated else "gateway"
    result = new_result(mode, args, meta)
    result["client_count"] = args.clients
    result["node_count"] = 3 if replicated else 1
    result["durability"] = ("majority Raft commit and local apply; durable per-node storage"
                            if replicated else "single-node in-memory; no process-restart durability")
    ports = [args.port]
    processes = []
    cluster_root = None
    if replicated:
        ports, processes, cluster_root = replicated_cluster(args)
    elif not args.external_gateway:
        port, process = start_single_gateway(args)
        ports = [port]
        processes = [process]
    monitor = ProcessMonitor(processes) if processes else None
    latencies = []
    errors = rejected = timeouts = completed = 0
    start_usage = resource.getrusage(resource.RUSAGE_SELF)
    start = time.monotonic()
    deadline = start + args.duration if args.duration > 0 else None
    target = args.operations
    n = 0
    client_count = max(1, args.clients)
    sockets = []
    try:
        for i in range(client_count):
            port = ports[i % len(ports)] if replicated else ports[0]
            try:
                sockets.append(connect_client(args.host, port, args.timeout))
            except OSError:
                sockets.append(None)
        while (n < target if deadline is None else time.monotonic() < deadline):
            index = n
            n += 1
            order = order_for(index, args.seed, client_count)
            sock_idx = index % client_count
            sock = sockets[sock_idx]
            request_frame = protocol_frame(order)
            sent = False
            if sock is None:
                port = ports[sock_idx % len(ports)] if replicated else ports[0]
                try:
                    sock = connect_client(args.host, port, args.timeout)
                    sockets[sock_idx] = sock
                except OSError:
                    errors += 1
                    timeouts += 1
                    continue
            before = time.monotonic_ns()
            try:
                sock.sendall(request_frame)
                code, _ = parse_order_response(sock, order[1])
                elapsed = time.monotonic_ns() - before
                if code == 0:
                    latencies.append(elapsed)
                    completed += 1
                elif replicated and code == 6:
                    # Drop the follower connection and retry the identical request
                    # through a fresh connection to every endpoint until resolved/deadline.
                    try:
                        sock.close()
                    except OSError:
                        pass
                    sockets[sock_idx] = None
                    deadline_retry = time.monotonic() + args.timeout
                    acknowledged = False
                    attempts = 0
                    while time.monotonic() < deadline_retry and attempts < max(3, len(ports) * 4):
                        port = ports[(attempts + 1) % len(ports)]
                        attempts += 1
                        retry_sock = None
                        try:
                            retry_sock = connect_client(args.host, port, min(args.timeout, 1.0))
                            retry_sock.sendall(request_frame)
                            retry_code, _ = parse_order_response(retry_sock, order[1])
                            if retry_code == 0:
                                latencies.append(time.monotonic_ns() - before)
                                completed += 1
                                acknowledged = True
                                break
                            if retry_code != 6:
                                rejected += 1
                                acknowledged = True
                                break
                        except (OSError, RuntimeError, TimeoutError, socket.timeout):
                            timeouts += 1
                        finally:
                            if retry_sock:
                                retry_sock.close()
                        time.sleep(0.01)
                    if not acknowledged:
                        rejected += 1
                    else:
                        # Reconnect the logical client slot to the original endpoint for next request.
                        try:
                            sockets[sock_idx] = connect_client(args.host,
                                ports[sock_idx % len(ports)] if replicated else ports[0], args.timeout)
                        except OSError:
                            sockets[sock_idx] = None
                else:
                    rejected += 1
            except (OSError, RuntimeError, TimeoutError, socket.timeout):
                errors += 1
                timeouts += 1
                try:
                    sock.close()
                except OSError:
                    pass
                sockets[sock_idx] = None
        elapsed_total = time.monotonic() - start
    finally:
        for sock in sockets:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        node_metrics = monitor.finish() if monitor else {}
        if replicated:
            stop_cluster(processes, cluster_root)
        elif processes:
            stop_process(processes[0])
    notes = ["Gateway workload is closed-loop: each client waits for its response before sending its next request.",
             "Latency uses local monotonic elapsed time around encode/send/response; no one-way cross-host timing."]
    result = finalize(result, latencies, completed, elapsed_total, start_usage, notes) | node_metrics | {
        "errors": errors, "rejected": rejected, "timeouts": timeouts,
        "generator": "closed-loop"}
    result["raw_latency_csv"] = write_latency_csv(args, mode, latencies)
    return result


def reserve_ports(count):
    sockets = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        return [s.getsockname()[1] for s in sockets]
    finally:
        for item in sockets:
            item.close()


def replicated_cluster(args):
    ports = reserve_ports(3)
    root = Path(tempfile.mkdtemp(prefix="atlas-perf-"))
    processes = []
    exe = Path(args.bin_dir) / "atlas-node"
    try:
        for i, port in enumerate(ports):
            cmd = [str(exe), "--replicated", "--node-id", str(i + 1), "--bind", args.host,
                   "--port", str(port), "--storage-dir", str(root / f"node-{i + 1}"),
                   "--heartbeat-ms", "35", "--election-min-ms", "180", "--election-max-ms", "320"]
            for peer in range(3):
                if peer != i:
                    cmd += ["--peer", f"{peer + 1},{args.host},{ports[peer]}"]
            processes.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        deadline = time.monotonic() + args.timeout * 4
        ready = False
        while time.monotonic() < deadline:
            if any(p.poll() is not None for p in processes):
                raise RuntimeError("replicated atlas-node exited during startup")
            for port in ports:
                try:
                    with connect_client(args.host, port, 0.25):
                        ready = True
                        break
                except OSError:
                    pass
            if ready:
                time.sleep(0.6)  # allow the fixed cluster to complete its initial election
                return ports, processes, root
            time.sleep(0.03)
        raise TimeoutError("replicated cluster did not become reachable before startup timeout")
    except Exception:
        for process in processes:
            process.terminate()
        for process in processes:
            try: process.wait(timeout=2)
            except subprocess.TimeoutExpired: process.kill()
        shutil.rmtree(root, ignore_errors=True)
        raise


def stop_cluster(processes, root):
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
    shutil.rmtree(root, ignore_errors=True)


def run_open_loop(args, meta, replicated=False):
    mode = "replicated" if replicated else "gateway"
    result = new_result(mode, args, meta)
    result["client_count"] = args.clients
    result["node_count"] = 3 if replicated else 1
    result["durability"] = ("majority Raft commit and local apply; durable per-node storage"
                            if replicated else "single-node in-memory; no process-restart durability")
    ports = [args.port]
    processes = []
    root = None
    if replicated:
        ports, processes, root = replicated_cluster(args)
    elif not args.external_gateway:
        port, process = start_single_gateway(args)
        ports = [port]
        processes = [process]
    monitor = ProcessMonitor(processes) if processes else None
    latencies = []
    errors = rejected = timeouts = completed = 0
    start_usage = resource.getrusage(resource.RUSAGE_SELF)
    executor = None
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        executor = ThreadPoolExecutor(max_workers=args.clients)
        start = time.monotonic()
        start_ns = time.monotonic_ns()
        interval = 1.0 / args.rate if args.rate > 0 else 0.0
        futures = []

        def perform(index, scheduled_ns):
            request = order_for(index, args.seed, args.clients)
            frame = protocol_frame(request)
            first_endpoint = index % len(ports) if replicated else 0
            endpoint_order = [first_endpoint] + [i for i in range(len(ports)) if i != first_endpoint]
            last_code = None
            retry_deadline = time.monotonic() + args.timeout * 4
            while time.monotonic() < retry_deadline:
                endpoint = endpoint_order[0]
                endpoint_order = endpoint_order[1:] + endpoint_order[:1]
                try:
                    with connect_client(args.host, ports[endpoint], args.timeout) as sock:
                        sock.sendall(frame)
                        code, response_id = read_order_response(sock)
                    if response_id != request[1]:
                        raise RuntimeError("response request ID mismatch")
                    last_code = code
                    if code == 0:
                        return "accepted", time.monotonic_ns() - scheduled_ns
                    if code != 6 or not replicated:
                        return "rejected", None
                except (OSError, RuntimeError, TimeoutError, socket.timeout):
                    pass
                if replicated:
                    time.sleep(0.01)
                else:
                    break
            return ("rejected", None) if last_code is not None else ("timeout", None)

        for index in range(args.operations):
            due = start + index * interval
            remaining = due - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            scheduled_ns = start_ns + int(index * interval * 1_000_000_000)
            futures.append(executor.submit(perform, index, scheduled_ns))

        try:
            for future in as_completed(futures, timeout=max(args.timeout * 4, 20.0)):
                try:
                    status, latency = future.result()
                except Exception:
                    status, latency = "timeout", None
                if status == "accepted":
                    completed += 1
                    latencies.append(latency)
                elif status == "rejected":
                    rejected += 1
                else:
                    errors += 1
                    timeouts += 1
        except TimeoutError:
            pass
        elapsed = time.monotonic() - start
        missing = args.operations - completed - rejected - errors
        if missing > 0:
            errors += missing
            timeouts += missing
        node_metrics = monitor.finish() if monitor else {}
        monitor = None
        result = finalize(result, latencies, completed, elapsed, start_usage,
                          ["Open-loop: requests scheduled independent of prior completion and executed with a bounded worker pool.",
                           "Latency measures scheduled-send to response using a local monotonic clock; TCP connection setup is included.",
                           "No cross-machine one-way latency is inferred."]) | node_metrics
        result.update({"errors": errors, "rejected": rejected, "timeouts": timeouts,
                       "generator": "open-loop", "target_rate_ops_s": args.rate})
        if missing:
            result["status"] = "incomplete"
            result["notes"].append(f"incomplete responses: {missing} requests lacked a classified response")
        result["raw_latency_csv"] = write_latency_csv(args, mode, latencies)
        return result
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        if replicated:
            stop_cluster(processes, root)
        elif processes:
            if monitor:
                monitor.finish()
            stop_process(processes[0])


def metadata(args):
    cache = Path(args.bin_dir) / "CMakeCache.txt"
    cache_values = {}
    if cache.exists():
        for line in cache.read_text(errors="replace").splitlines():
            if ":" in line and "=" in line and not line.startswith("//"):
                key, value = line.split("=", 1)
                cache_values[key.split(":", 1)[0]] = value
    compiler = cache_values.get("CMAKE_CXX_COMPILER", args.compiler)
    try:
        compiler_line = subprocess.run([compiler, "--version"], capture_output=True, text=True,
                                       timeout=2).stdout.splitlines()[0]
    except Exception:
        compiler_line = args.compiler_version
    vm_info = args.vm
    if vm_info == "auto":
        dmi = Path("/sys/class/dmi/id/product_name")
        vm_info = dmi.read_text().strip() if dmi.exists() else "unavailable"
    meminfo = Path("/proc/meminfo")
    visible_memory_kib = None
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                visible_memory_kib = int(line.split()[1])
                break
    return {"compiler": compiler, "compiler_version": compiler_line,
            "build_type": cache_values.get("CMAKE_BUILD_TYPE", args.build_type), "vm_configuration": vm_info,
            "visible_memory_kib": visible_memory_kib,
            "platform": platform.platform(), "python": platform.python_version(),
            "cpu_count": os.cpu_count(), "kernel": platform.release()}


def write_jsonl(path, records):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        for item in records:
            handle.write(json.dumps(item, sort_keys=True) + "\n")


def write_latency_csv(args, mode, latencies):
    path = Path(args.output_dir) / f"raw_{mode}_latency_{args.seed}_{time.time_ns()}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("sample_index", "latency_ns", "seed", "generator", "mode"))
        for index, latency in enumerate(latencies):
            writer.writerow((index, latency, args.seed, args.generator, mode))
    return str(path)


def write_sample_csv(args, mode, samples):
    path = Path(args.output_dir) / f"raw_{mode}_samples_{args.seed}_{time.time_ns()}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("sample_index", "operation", "latency_ns", "success", "trades", "seed"))
        for index, item in enumerate(samples):
            writer.writerow((index, item["operation"], item["latency_ns"], int(item["success"]), item["trades"], args.seed))
    return str(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--bin-dir", default=str(DEFAULT_BIN))
    parser.add_argument("--output-dir", default=str(ROOT / "benchmarks" / "raw"))
    parser.add_argument("--output", default=None, help="JSONL result file (default: output-dir/results.jsonl)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--external-gateway", action="store_true",
                        help="use a manually started gateway at --host/--port instead of launching nodes")
    parser.add_argument("--operations", type=int, default=10000)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--duration", type=float, default=0.0, help="duration in seconds; >0 uses duration-based closed loop")
    parser.add_argument("--rate", type=float, default=1000.0, help="open-loop target aggregate request rate")
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--seed", type=int, default=7245981)
    parser.add_argument("--distribution", default="50% buy, 50% sell; deterministic prices/quantities")
    parser.add_argument("--generator", choices=("closed-loop", "open-loop"), default="closed-loop")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--compiler", default="GNU C++")
    parser.add_argument("--compiler-version", default="see CMake compiler configuration")
    parser.add_argument("--build-type", default="Release")
    parser.add_argument("--vm", default="auto", help="VM/model description (auto reads DMI product name)")
    args = parser.parse_args()
    if args.operations < 1 or args.clients < 1 or args.trials < 1 or args.timeout <= 0:
        parser.error("operations, clients, trials, and timeout must be positive")
    if args.mode not in ("gateway", "replicated") and args.generator == "open-loop":
        parser.error("open-loop generation is only implemented for gateway and replicated modes")
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    meta = metadata(args)
    gateway_runner = (lambda: run_open_loop(args, meta)) if args.generator == "open-loop" else (lambda: run_gateway(args, meta))
    replicated_runner = (lambda: run_open_loop(args, meta, replicated=True)) if args.generator == "open-loop" else (lambda: run_gateway(args, meta, replicated=True))
    runners = {"matching": lambda: run_cli_benchmark(args, meta, "matching"), "gateway": gateway_runner,
               "wal": lambda: run_wal(args, meta), "raft": lambda: run_raft(args, meta),
               "replicated": replicated_runner,
               "snapshot": lambda: run_snapshot(args, meta), "mixed": lambda: run_mixed(args, meta)}
    records = []
    # Independent trials get different recorded deterministic seeds.
    for trial in range(args.trials):
        args.seed = (args.seed + trial) & ((1 << 64) - 1)
        record = runners[args.mode]()
        record["trial"] = trial + 1
        record["trial_seed"] = args.seed
        record["config"]["requested_operations"] = args.operations
        record["config"]["requested_clients"] = args.clients
        record["config"]["requested_duration_s"] = args.duration
        record["config"]["generator"] = args.generator
        record["config"]["target_rate_ops_s"] = args.rate
        records.append(record)
        print(json.dumps(record, sort_keys=True))
    output = args.output or str(Path(args.output_dir) / "results.jsonl")
    write_jsonl(output, records)
    print(f"raw_results={output}")


if __name__ == "__main__":
    main()
