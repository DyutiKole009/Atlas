#!/usr/bin/env python3
"""Summarize raw Atlas benchmark JSONL trials; standard library only."""
import argparse
import json
import statistics
from pathlib import Path


def pct(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int((p / 100) * (len(ordered) - 1))))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="JSONL file produced by atlas_perf.py")
    args = parser.parse_args()
    rows = [json.loads(line) for line in Path(args.input).read_text().splitlines() if line.strip()]
    groups = {}
    for row in rows:
        key = (row["mode"], row.get("generator", "n/a"))
        groups.setdefault(key, []).append(row)
    print("mode,generator,trials,valid_trials,completed_per_s_mean,completed_per_s_stdev,p50_ns_median,p95_ns_median,p99_ns_median,p999_ns_median,errors,rejected,timeouts")
    for (mode, generator), trials in sorted(groups.items()):
        valid = [r for r in trials if r.get("status") == "ok" and not r.get("errors")
             and not r.get("rejected") and not r.get("timeouts")
             and (r.get("duration_target_s", 0) > 0 or
                  r.get("completed", 0) + r.get("rejected", 0) + r.get("errors", 0) ==
                   r.get("operations_requested", 0))]
        rates = [r["throughput_ops_s"] for r in valid]
        def median_metric(k):
            values = [r.get("latency", {}).get(k) for r in valid if r.get("latency", {}).get(k) is not None]
            return statistics.median(values) if values else ""
        print(f"{mode},{generator},{len(trials)},{len(valid)},{statistics.mean(rates) if rates else ''},"
              f"{statistics.stdev(rates) if len(rates)>1 else 0 if rates else ''},"
              f"{median_metric('p50_ns')},{median_metric('p95_ns')},{median_metric('p99_ns')},"
              f"{median_metric('p999_ns')},{sum(r.get('errors',0) for r in trials)},"
              f"{sum(r.get('rejected',0) for r in trials)},{sum(r.get('timeouts',0) for r in trials)}")


if __name__ == "__main__":
    main()
