#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${WEB_TEST_PORT:-18090}"
BASE_URL="http://127.0.0.1:$PORT"
TMP_DIR="$(mktemp -d)"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

cd "$ROOT_DIR"
python3 web/server.py --port "$PORT" >"$TMP_DIR/server.log" 2>&1 &
SERVER_PID="$!"
for _ in $(seq 1 40); do
  if curl -fsS "$BASE_URL/api/dashboard" >"$TMP_DIR/dashboard.json" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
curl -fsS "$BASE_URL/api/dashboard" >"$TMP_DIR/dashboard.json"
curl -fsS "$BASE_URL/" >"$TMP_DIR/index.html"
curl -fsS "$BASE_URL/styles.css" >"$TMP_DIR/styles.css"
curl -fsS "$BASE_URL/app.js" >"$TMP_DIR/app.js"
curl -fsS "$BASE_URL/api/datasets/csv" >"$TMP_DIR/csv.json"
curl -fsS "$BASE_URL/api/datasets/csv?path=tests/fixtures/paper/smartphone-paper-small.csv" >"$TMP_DIR/paper.csv.json"
curl -fsS "$BASE_URL/api/datasets/osm-readiness" >"$TMP_DIR/osm.json"
curl -fsS "$BASE_URL/api/experiment-matrix" >"$TMP_DIR/matrix.json"
curl -fsS "$BASE_URL/api/reports/all-dataset" >"$TMP_DIR/all-dataset.json"
curl -fsS "$BASE_URL/api/reports/hadoop-reference" >"$TMP_DIR/hadoop-reference.json"
curl -fsS "$BASE_URL/api/reports/latex" >"$TMP_DIR/results.tex"

grep -q "PTD-BenchLab" "$TMP_DIR/index.html"
grep -q "Runs &amp; Compare" "$TMP_DIR/index.html"
grep -q "Rai-Lian baseline and ICCIT Spark upgrade" "$TMP_DIR/index.html"
grep -q "AES+DSCP comparison guard" "$TMP_DIR/index.html"
grep -q "Baseline vs ICCIT Hadoop and Spark treatments" "$TMP_DIR/index.html"
grep -q "All-dataset comparison report" "$TMP_DIR/index.html"
grep -q "\\.drawer" "$TMP_DIR/styles.css"
grep -q "renderDashboard" "$TMP_DIR/app.js"
python3 - "$TMP_DIR/dashboard.json" "$TMP_DIR/csv.json" "$TMP_DIR/paper.csv.json" "$TMP_DIR/osm.json" "$TMP_DIR/matrix.json" "$TMP_DIR/all-dataset.json" "$TMP_DIR/hadoop-reference.json" <<'PY'
import json
import sys

with open(sys.argv[1]) as source:
  dashboard = json.load(source)
with open(sys.argv[2]) as source:
  dataset = json.load(source)
with open(sys.argv[3]) as source:
  paper = json.load(source)
with open(sys.argv[4]) as source:
  osm = json.load(source)
with open(sys.argv[5]) as source:
  matrix = json.load(source)
with open(sys.argv[6]) as source:
  all_dataset = json.load(source)
with open(sys.argv[7]) as source:
  hadoop_reference = json.load(source)
assert dashboard["project"]["name"] == "PTD-BenchLab"
assert "savedRuns" in dashboard["counts"]
assert dataset["records"] == 12
assert dataset["missingAttributeValues"] > 0
assert paper["probabilityAudit"]["passed"] is True
assert osm["ready"] is True
assert matrix["runCount"] > 0
assert "Hadoop" in all_dataset["claimBoundary"]
assert "treatments" in all_dataset and "streams" in all_dataset
assert "aes+dhcp" in hadoop_reference["aliasNote"]
assert len(hadoop_reference["publishedHadoop"]) >= 2
assert "sparkSuites" in hadoop_reference
PY
grep -q '\\begin{tabular}' "$TMP_DIR/results.tex"

mapfile -t run_ids < <(
  find reports/runs -mindepth 2 -maxdepth 2 -name manifest.json -printf '%h\n' |
    xargs -r -n1 basename | sort | head -n 2
)
if ((${#run_ids[@]} > 0)); then
  run_id="${run_ids[0]}"
  curl -fsS "$BASE_URL/api/runs/$run_id" >"$TMP_DIR/run.json"
  curl -fsS "$BASE_URL/api/runs/$run_id/bundle" >"$TMP_DIR/bundle.zip"
  python3 - "$TMP_DIR/run.json" "$TMP_DIR/bundle.zip" "$run_id" <<'PY'
import json
import sys
import zipfile

with open(sys.argv[1]) as source:
  run = json.load(source)
assert run["id"] == sys.argv[3]
with zipfile.ZipFile(sys.argv[2]) as archive:
  assert f"{sys.argv[3]}/manifest.json" in archive.namelist()
PY
fi
if ((${#run_ids[@]} > 1)); then
  curl -fsS "$BASE_URL/api/compare?ids=${run_ids[0]},${run_ids[1]}" >"$TMP_DIR/comparison.json"
  python3 - "$TMP_DIR/comparison.json" <<'PY'
import json
import sys

with open(sys.argv[1]) as source:
  comparison = json.load(source)
assert len(comparison["runs"]) == 2
assert "fair" in comparison
PY
fi

echo "validated PTD-BenchLab HTTP endpoints at $BASE_URL"
