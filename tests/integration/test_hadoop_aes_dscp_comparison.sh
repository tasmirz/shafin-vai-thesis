#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUITE_ID="${SUITE_ID:-hadoop-aes-dscp-test-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
CSV_PATH="${CSV_PATH:-$ROOT_DIR/tests/fixtures/csv/smartphone-small.csv}"

cd "$ROOT_DIR"

PROFILE="${PROFILE:-smartphone}" \
  SETUP=paired \
  SUITE_ID="$SUITE_ID" \
  CSV_PATH="$CSV_PATH" \
  K="${K:-2}" \
  PARTITIONS="${PARTITIONS:-2}" \
  VALIDATE_EXACT="${VALIDATE_EXACT:-true}" \
  BUILD_IMAGE="${BUILD_IMAGE:-1}" \
  TRACE_LIMIT="${TRACE_LIMIT:-10}" \
  scripts/research/run_paper_setup.sh

REPORT="$ROOT_DIR/reports/validation/${SUITE_ID}-paired.md"
BASE_METRICS="$ROOT_DIR/reports/runs/${SUITE_ID}-rai-lian-baseline/metrics.json"
AES_DSCP_METRICS="$ROOT_DIR/reports/runs/${SUITE_ID}-iccit-aes-dscp/metrics.json"

test -f "$REPORT"
test -f "$BASE_METRICS"
test -f "$AES_DSCP_METRICS"

grep -Fq "Published ICCIT Hadoop AES+DSCP" "$REPORT"
grep -Fq "Spark ICCIT AES+DSCP" "$REPORT"
grep -Fq '(`aes-dscp`)' "$REPORT"
grep -Fq "reference only" "$REPORT"
grep -Fq "not combined with Spark timings as an engine speedup" "$REPORT"

python3 - "$BASE_METRICS" "$AES_DSCP_METRICS" <<'PY'
import json
import sys
from pathlib import Path

baseline = json.loads(Path(sys.argv[1]).read_text())
upgrade = json.loads(Path(sys.argv[2]).read_text())

assert baseline["spark"]["algorithm"] == "baseline"
assert upgrade["spark"]["algorithm"] == "aes-dscp"
assert upgrade["spark"]["dscpEnabled"] is True
assert upgrade["spark"]["aesEnabled"] is True
assert upgrade["validation"]["exactTopKAgreement"] is True
assert upgrade["validation"]["queriesChecked"] > 0
assert all(query["algorithm"] == "aes-dscp" for query in upgrade["spark"]["queries"])
assert all(query["falsePrunes"] == 0 for query in upgrade["spark"]["queries"])

print(
    "hadoopReferenceComparisonValidated=true "
    f"suite={Path(sys.argv[2]).parent.name} "
    f"baselineMs={baseline['spark']['algorithmElapsedMs']} "
    f"aesDscpMs={upgrade['spark']['algorithmElapsedMs']}"
)
PY
