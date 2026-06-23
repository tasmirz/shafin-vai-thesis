#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export SPARK_SUITE_ID="spark-ablation-$(date -u +%Y%m%dT%H%M%SZ)"
export HADOOP_SUITE_ID="hadoop-ablation-$(date -u +%Y%m%dT%H%M%SZ)"

echo "== Starting Three-Way Comparison (Spark vs ICCIT Hadoop) =="
echo "Spark Suite ID:  $SPARK_SUITE_ID"
echo "Hadoop Suite ID: $HADOOP_SUITE_ID"

echo ""
echo "=> Phase 1: Running Spark Ablation Suite"
REQUIRE_PRUNING=false SUITE_ID="$SPARK_SUITE_ID" "$ROOT_DIR/scripts/research/run_ablation_suite.sh"

echo ""
echo "=> Phase 2: Running ICCIT Hadoop Ablation Suite"
SUITE_ID="$HADOOP_SUITE_ID" "$ROOT_DIR/scripts/research/run_hadoop_ablation_suite.sh"

echo ""
echo "=> Phase 3: Rendering Three-Way Comparison Report"
python3 "$ROOT_DIR/scripts/research/render_three_way_comparison.py" \
  --spark-suite "$SPARK_SUITE_ID" \
  --hadoop-suite "$HADOOP_SUITE_ID"

echo "== Comparison Complete =="
