#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/benchmarks/results/perf-$(date -u +%Y%m%dT%H%M%SZ)}"
SEED="${SEED:-7245981}"
TRIALS="${TRIALS:-5}"
CLIENTS="${CLIENTS:-4}"
RATE="${RATE:-1000}"
DURATION="${DURATION:-0}"
VM="${VM:-auto}"
mkdir -p "$OUT/raw"

cmake -S "$ROOT" -B "$ROOT/build" -DCMAKE_BUILD_TYPE=Release
cmake --build "$ROOT/build" --parallel
ctest --test-dir "$ROOT/build" --output-on-failure

mode_index=0
for mode in matching wal raft snapshot mixed gateway replicated; do
  case "$mode" in
    matching) default_ops=100000; mode_ops="${MATCHING_OPS:-${OPS:-$default_ops}}" ;;
    wal) default_ops=2000; mode_ops="${WAL_OPS:-${OPS:-$default_ops}}" ;;
    raft) default_ops=3000; mode_ops="${RAFT_OPS:-${OPS:-$default_ops}}" ;;
    snapshot) default_ops=1000; mode_ops="${SNAPSHOT_OPS:-${OPS:-$default_ops}}" ;;
    mixed) default_ops=50000; mode_ops="${MIXED_OPS:-${OPS:-$default_ops}}" ;;
    gateway) default_ops=2000; mode_ops="${GATEWAY_OPS:-${OPS:-$default_ops}}" ;;
    replicated) default_ops=300; mode_ops="${REPLICATED_OPS:-${OPS:-$default_ops}}" ;;
  esac
  generator=closed-loop
  if [[ "$mode" == gateway || "$mode" == replicated ]]; then
    generator="${GENERATOR:-closed-loop}"
  fi
  mode_seed=$((SEED + mode_index))
  /usr/bin/python3 "$ROOT/scripts/atlas_perf.py" \
    --mode "$mode" --bin-dir "$ROOT/build" --output-dir "$OUT/raw" \
    --output "$OUT/results.jsonl" --operations "$mode_ops" --clients "$CLIENTS" \
    --trials "$TRIALS" --seed "$mode_seed" --rate "$RATE" --duration "$DURATION" \
    --generator "$generator" --vm "$VM"
  mode_index=$((mode_index + 1))
done
/usr/bin/python3 "$ROOT/scripts/analyze_atlas_perf.py" "$OUT/results.jsonl" > "$OUT/summary.csv"
printf 'results=%s\n' "$OUT/results.jsonl"
printf 'summary=%s\n' "$OUT/summary.csv"
