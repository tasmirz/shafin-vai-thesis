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
    
    # Load Spark runs
    spark_runs = []
    for d in reports_dir.glob(f"{args.spark_suite}-*"):
        f = d / "metrics.json"
        if f.is_file():
            with open(f) as fp:
                data = json.load(fp)
                spark_runs.append(data.get("spark", data))
            
    # Load Hadoop runs
    hadoop_runs = []
    for d in reports_dir.glob(f"{args.hadoop_suite}-*"):
        f = d / "metrics.json"
        if f.is_file():
            with open(f) as fp:
                data = json.load(fp)
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

    spark_runs.sort(key=get_order)
    hadoop_runs.sort(key=get_order)

    print("\n== Three-Way Comparison: Spark vs ICCIT Hadoop ==")
    print(f"Dataset: {spark_runs[0].get('datasetPath', 'unknown')}")
    print(f"K: {spark_runs[0].get('k', '?')} | Partitions: {spark_runs[0].get('partitions', '?')}")
    print()
    print("| Engine | Variant | Algorithm ms | Setup ms | Total ms | Prune Ratio | Emitted Records | False Prunes | Exact Match |")
    print("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")

    # Combine runs for display: we consider "Baseline Spark" as Spark baseline, "New Spark" as Spark aes-dscp, etc.
    # But for a full 3-way, we just show all treatments side by side for both engines
    
    for r in spark_runs:
        algo = r.get("algorithm", "unknown")
        algo_ms = r.get("algorithmElapsedMs", 0)
        setup_ms = r.get("setupMs", 0)
        total_ms = r.get("elapsedMs", algo_ms + setup_ms)
        prune_ratio = r.get("avgPruneRatio", 0.0)
        emissions = r.get("totalEmittedRecords", 0)
        false_prunes = r.get("falsePruneCount", 0)
        exact_match = "True" if r.get("validation", {}).get("exactTopKAgreement") else "False"
        if not r.get("validation"):
            exact_match = "True" if r.get("exactAgreement") else "False" # Fallback if inside query
        print(f"| Spark | {algo} | {algo_ms} | {setup_ms} | {total_ms} | {prune_ratio:.4f} | {emissions} | {false_prunes} | {exact_match} |")

    for r in hadoop_runs:
        algo = r.get("algorithm", "unknown")
        algo_ms = r.get("algorithmElapsedMs", 0)
        setup_ms = r.get("setupMs", 0)
        total_ms = r.get("elapsedMs", algo_ms + setup_ms)
        prune_ratio = r.get("avgPruneRatio", 0.0)
        emissions = r.get("totalEmittedRecords", 0)
        false_prunes = r.get("falsePruneCount", 0)
        exact_match = "True" if r.get("validation", {}).get("exactTopKAgreement") else "False"
        if not r.get("validation"):
            exact_match = "True" if r.get("exactAgreement") else "False"
        print(f"| Hadoop | {algo} | {algo_ms} | {setup_ms} | {total_ms} | {prune_ratio:.4f} | {emissions} | {false_prunes} | {exact_match} |")

    print("\nSpeedup Analysis:")
    spark_baseline = next((r for r in spark_runs if r.get("algorithm") == "baseline"), None)
    spark_new = next((r for r in spark_runs if r.get("algorithm") == "aes-dscp"), None)
    hadoop_baseline = next((r for r in hadoop_runs if r.get("algorithm") == "baseline"), None)
    hadoop_iccit = next((r for r in hadoop_runs if r.get("algorithm") == "aes-dscp"), None)

    if spark_baseline and spark_new:
        sb_ms = spark_baseline.get("elapsedMs", 1)
        sn_ms = spark_new.get("elapsedMs", 1)
        print(f"- New Spark (AES+DSCP) is {sb_ms/sn_ms:.2f}x faster than Baseline Spark")

    if hadoop_baseline and hadoop_iccit:
        hb_ms = hadoop_baseline.get("elapsedMs", 1)
        hi_ms = hadoop_iccit.get("elapsedMs", 1)
        print(f"- ICCIT Hadoop (AES+DSCP) is {hb_ms/hi_ms:.2f}x faster than Baseline Hadoop")
        
    if spark_new and hadoop_iccit:
        sn_ms = spark_new.get("elapsedMs", 1)
        hi_ms = hadoop_iccit.get("elapsedMs", 1)
        if sn_ms < hi_ms:
            print(f"- New Spark is {hi_ms/sn_ms:.2f}x faster than ICCIT Hadoop")
        else:
            print(f"- ICCIT Hadoop is {sn_ms/hi_ms:.2f}x faster than New Spark")

if __name__ == "__main__":
    main()
