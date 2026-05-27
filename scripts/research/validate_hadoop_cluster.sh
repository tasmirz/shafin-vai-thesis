#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.hadoop.yml"
OUTPUT_DIR="$ROOT_DIR/reports/hadoop"
BUILD_IMAGE="${BUILD_IMAGE:-1}"

mkdir -p "$OUTPUT_DIR"
cd "$ROOT_DIR"
if [[ "$BUILD_IMAGE" == "1" || "$BUILD_IMAGE" == "true" ]]; then
  docker compose -f "$COMPOSE_FILE" build
fi
docker compose -f "$COMPOSE_FILE" up -d namenode datanode resourcemanager nodemanager historyserver

for _ in {1..60}; do
  if docker compose -f "$COMPOSE_FILE" run --rm hadoop-client \
      client bash -lc 'hdfs dfs -ls / >/dev/null && yarn node -list >/dev/null' \
      >"$OUTPUT_DIR/cluster-smoke.log" 2>&1; then
    break
  fi
  sleep 2
done

docker compose -f "$COMPOSE_FILE" run --rm hadoop-client client bash -lc \
  'hdfs dfsadmin -safemode wait; hdfs dfs -rm -r -f /tmp/benchlab-smoke >/dev/null 2>&1 || true; hdfs dfs -mkdir -p /tmp/benchlab-smoke/input; printf "ptd baseline\nptd aes dscp\n" | hdfs dfs -put -f - /tmp/benchlab-smoke/input/smoke.txt; hadoop jar "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /tmp/benchlab-smoke/input /tmp/benchlab-smoke/output; hdfs dfs -cat /tmp/benchlab-smoke/output/part-r-00000' \
  | tee "$OUTPUT_DIR/mapreduce-smoke.log"

cat >"$OUTPUT_DIR/README.md" <<'EOF'
# Hadoop MapReduce Cluster Smoke Evidence

This artifact validates HDFS and YARN MapReduce execution in the isolated Hadoop Compose
environment. It does **not** claim a PTD baseline run. A comparable Hadoop-vs-Spark PTD matrix
requires a true MapReduce implementation of Rai-Lian/ICCIT filtering, reducer and refinement
jobs using the same curated datasets and parameters.
EOF
echo "hadoopMapReduceInfrastructure=validated report=$OUTPUT_DIR/mapreduce-smoke.log"
