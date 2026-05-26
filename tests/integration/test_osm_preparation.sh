#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_PATH="${REPORT_PATH:-/tmp/bangladesh-osm-readiness.json}"

cd "$ROOT_DIR"
python3 scripts/research/validate_osm_source.py --output "$REPORT_PATH" >/tmp/bangladesh-osm-readiness.stdout
python3 - "$REPORT_PATH" <<'PY'
import json
import sys
from pathlib import Path

report = json.loads(Path(sys.argv[1]).read_text())
assert report["readyForCuration"] is True
assert report["source"]["lineFeatureCount"] >= 98451
assert report["source"]["snapshot"] == "2026-05-10"
assert report["curationStatus"].startswith("pending")
print(
    "osmSourceReady=true "
    f"lineFeatures={report['source']['lineFeatureCount']} "
    f"snapshot={report['source']['snapshot']}")
PY
