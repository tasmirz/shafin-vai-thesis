#!/usr/bin/env python3
import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Render Three-Way Comparison: Baseline Spark vs New Spark vs ICCIT Hadoop")
    parser.add_argument("--spark-suite", required=True, help="Spark ablation suite ID (e.g., ablation-2026...)")
    parser.add_argument("--hadoop-suite", required=True, help="Hadoop ablation suite ID (e.g., hadoop-ablation-2026...)")
    args = parser.parse_args()

    reports_dir = Path("reports/runs")
    
    # Load Spark runs — keep both the full JSON (for validation key) and the 'spark' metrics sub-key
    spark_runs = []
    _spark_full = []
    for d in sorted(reports_dir.glob(f"{args.spark_suite}-*")):
        f = d / "metrics.json"
        if f.is_file():
            with open(f) as fp:
                data = json.load(fp)
                _spark_full.append(data)
                spark_runs.append(data.get("spark", data))
            
    # Load Hadoop runs
    hadoop_runs = []
    _hadoop_full = []
    for d in sorted(reports_dir.glob(f"{args.hadoop_suite}-*")):
        f = d / "metrics.json"
        if f.is_file():
            with open(f) as fp:
                data = json.load(fp)
                _hadoop_full.append(data)
                # the test script wraps it in 'spark' key regardless of engine
                hadoop_runs.append(data.get("spark", data))

    if not spark_runs or not hadoop_runs:
        print(f"Error: Missing runs for suites {args.spark_suite} or {args.hadoop_suite}", file=sys.stderr)
        sys.exit(1)

    # Sort runs by algorithm
    order = ["baseline", "aes-only", "dscp-only", "aes-dscp"]
    def get_order(x):
        alg = x.get("algorithm", "baseline")
        return order.index(alg) if alg in order else 99

    paired = sorted(zip(spark_runs, _spark_full), key=lambda p: get_order(p[0]))
    spark_runs, _spark_full = [p[0] for p in paired], [p[1] for p in paired]
    paired = sorted(zip(hadoop_runs, _hadoop_full), key=lambda p: get_order(p[0]))
    hadoop_runs, _hadoop_full = [p[0] for p in paired], [p[1] for p in paired]

    print("\n== Three-Way Comparison: Spark vs ICCIT Hadoop ==")
    print(f"Dataset: {spark_runs[0].get('datasetPath', 'unknown')}")
    print(f"K: {spark_runs[0].get('k', '?')} | Partitions: {spark_runs[0].get('partitions', '?')}")
    print()
    print("| Engine | Variant | Algorithm ms | Setup ms | Total ms | Prune Ratio | Emitted Records | False Prunes | Exact Match |")
    print("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")

    # Combine runs for display: we consider "Baseline Spark" as Spark baseline, "New Spark" as Spark aes-dscp, etc.
    # But for a full 3-way, we just show all treatments side by side for both engines
    
    def fmt_row(engine, r, full_data):
        algo = r.get("algorithm", "unknown")
        algo_ms = r.get("algorithmElapsedMs", 0)
        setup_ms = r.get("setupMs", 0)
        total_ms = r.get("elapsedMs", algo_ms + setup_ms)
        prune_ratio = r.get("avgPruneRatio", 0.0)
        emissions = r.get("totalEmittedRecords", 0)
        false_prunes = r.get("falsePruneCount", 0)
        # validation lives at the TOP level of metrics.json, not inside 'spark' key
        val = full_data.get("validation", {})
        exact_match = "✅" if val.get("exactTopKAgreement") else "❌"
        return f"| {engine} | {algo} | {algo_ms} | {setup_ms} | {total_ms} | {prune_ratio:.4f} | {emissions} | {false_prunes} | {exact_match} |"

    for r, fd in zip(spark_runs, _spark_full):
        print(fmt_row("Spark", r, fd))

    for r, fd in zip(hadoop_runs, _hadoop_full):
        print(fmt_row("Hadoop", r, fd))

    print("\nSpeedup Analysis (algorithmElapsedMs = act WC per paper, excludes cluster startup):")
    spark_baseline = next((r for r in spark_runs if r.get("algorithm") == "baseline"), None)
    spark_new = next((r for r in spark_runs if r.get("algorithm") == "aes-dscp"), None)
    hadoop_baseline = next((r for r in hadoop_runs if r.get("algorithm") == "baseline"), None)
    hadoop_iccit = next((r for r in hadoop_runs if r.get("algorithm") == "aes-dscp"), None)
    hadoop_aes = next((r for r in hadoop_runs if r.get("algorithm") == "aes-only"), None)
    hadoop_dscp = next((r for r in hadoop_runs if r.get("algorithm") == "dscp-only"), None)

    def pct(a, b): return f"{(a - b) / a * 100:.1f}%" if a else "N/A"

    if hadoop_baseline and hadoop_iccit:
        hb = hadoop_baseline.get("algorithmElapsedMs", 1)
        hi = hadoop_iccit.get("algorithmElapsedMs", 1)
        ha = hadoop_aes.get("algorithmElapsedMs", 1) if hadoop_aes else 0
        hd = hadoop_dscp.get("algorithmElapsedMs", 1) if hadoop_dscp else 0
        print(f"  Hadoop AES-only   : {ha}ms  (vs baseline {hb}ms → {pct(hb, ha)} reduction)  [paper: ~32.7% synthetic]")
        print(f"  Hadoop DSCP-only  : {hd}ms  (vs baseline {hb}ms → {pct(hb, hd)} reduction)  [paper: ~12.0% synthetic]")
        print(f"  Hadoop AES+DSCP   : {hi}ms  (vs baseline {hb}ms → {pct(hb, hi)} reduction)  [paper: ~34.2% synthetic]")

    if spark_baseline and spark_new:
        sb = spark_baseline.get("algorithmElapsedMs", 1)
        sn = spark_new.get("algorithmElapsedMs", 1)
        print(f"  Spark AES+DSCP    : {sn}ms  (vs baseline {sb}ms)")

    if spark_new and hadoop_iccit:
        sn = spark_new.get("algorithmElapsedMs", 1)
        hi = hadoop_iccit.get("algorithmElapsedMs", 1)
        faster = "Spark" if sn < hi else "Hadoop"
        ratio = hi / sn if sn < hi else sn / hi
        print(f"  → {faster} AES+DSCP is {ratio:.2f}x faster (algorithm time only)")

if __name__ == "__main__":
    main()
