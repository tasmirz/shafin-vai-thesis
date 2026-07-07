#!/usr/bin/env python3
import json
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("Usage: generate_plots.py <SUITE_ID>")
        sys.exit(1)

    suite_id = sys.argv[1]
    root = Path("reports/runs")
    if not root.exists():
        print(f"Directory {root} does not exist.")
        sys.exit(1)

    # Algorithms to track
    algorithms = ["baseline", "dscp-only", "aes-only", "aes-dscp", "improved-baseline", "improved-dscp-only", "improved-aes-only", "improved-aes-dscp"]
    engines = ["hadoop", "spark"]

    data = []

    for engine in engines:
        for algo in algorithms:
            # Full suite format: SUITE_ID-engine-algo
            # Ablation suite format: SUITE_ID-algo (only runs spark)
            run_id = f"{suite_id}-{engine}-{algo}"
            metrics_path = root / run_id / "metrics.json"
            
            if not metrics_path.exists() and engine == "spark":
                run_id = f"{suite_id}-{algo}"
                metrics_path = root / run_id / "metrics.json"

            if metrics_path.exists():
                with open(metrics_path, 'r') as f:
                    metrics = json.load(f)
                    spark = metrics.get("spark", {})
                    validation = metrics.get("validation", {})
                    
                    elapsed = spark.get("algorithmElapsedMs")
                    emitted = spark.get("totalEmittedRecords")
                    pruning = spark.get("avgPruneRatio", 0) * 100
                    exact = validation.get("exactTopKAgreement", False)

                    if elapsed is not None:
                        data.append({
                            "Engine": engine,
                            "Algorithm": algo,
                            "Elapsed Time (ms)": elapsed,
                            "Emitted Records": emitted,
                            "Pruning Ratio (%)": pruning,
                            "Exact Agreement": "Yes" if exact else "No"
                        })

    if not data:
        print(f"No valid run data found for suite {suite_id}")
        sys.exit(1)

    df = pd.DataFrame(data)
    
    figures_dir = Path("reports/figures")
    figures_dir.mkdir(parents=True, exist_ok=True)

    # Set seaborn style
    sns.set_theme(style="whitegrid")

    # 1. Execution Time Plot
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x="Algorithm", y="Elapsed Time (ms)", hue="Engine", palette="viridis")
    plt.title(f"Algorithm Execution Time - Suite: {suite_id}")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(figures_dir / f"{suite_id}_time.png", dpi=150)
    plt.close()

    # 2. Emitted Records Plot
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x="Algorithm", y="Emitted Records", hue="Engine", palette="magma")
    plt.title(f"Total Emitted Records - Suite: {suite_id}")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(figures_dir / f"{suite_id}_emissions.png", dpi=150)
    plt.close()

    # 3. Pruning Ratio Plot (Spark only, since Hadoop is same usually)
    spark_df = df[df["Engine"] == "spark"]
    if not spark_df.empty:
        plt.figure(figsize=(10, 6))
        sns.barplot(data=spark_df, x="Algorithm", y="Pruning Ratio (%)", palette="crest")
        plt.title(f"Candidate Pruning Ratio (Spark) - Suite: {suite_id}")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(figures_dir / f"{suite_id}_pruning.png", dpi=150)
        plt.close()

    print(f"Plots generated successfully in {figures_dir}")

if __name__ == "__main__":
    main()
