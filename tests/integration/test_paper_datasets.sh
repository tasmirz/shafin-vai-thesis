#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
cd "$ROOT_DIR"

python3 scripts/research/build_paper_dataset.py smartphone \
  --output "$TEMP_DIR/smartphone.csv" --manifest "$TEMP_DIR/smartphone.json" \
  --objects 12 --instances-min 5 --instances-max 8 --queries 2 --partitions 3 --seed 42 >/dev/null
python3 scripts/research/build_paper_dataset.py validate --input "$TEMP_DIR/smartphone.csv" >/dev/null

for distribution in uniform gaussian zipf; do
  python3 scripts/research/build_paper_dataset.py synthetic \
    --distribution "$distribution" --lmax 8 --zipf-skew 0.8 \
    --output "$TEMP_DIR/$distribution.csv" --manifest "$TEMP_DIR/$distribution.json" \
    --objects 12 --instances-min 2 --instances-max 4 --queries 2 --partitions 3 --seed 42 >/dev/null
  python3 scripts/research/build_paper_dataset.py validate --input "$TEMP_DIR/$distribution.csv" >/dev/null
done

uv run --with pyproj scripts/research/build_paper_dataset.py osm \
  --source datasets-osm/hotosm_bgd_roads_osm_shp/roads_lines.shp \
  --output "$TEMP_DIR/roads.csv" --manifest "$TEMP_DIR/roads.json" \
  --objects 20 --instances-min 5 --instances-max 11 --queries 1 --partitions 3 --seed 42 >/dev/null
python3 scripts/research/build_paper_dataset.py validate --input "$TEMP_DIR/roads.csv" >/dev/null
python3 scripts/simulator.py --dataset=csv --dataset-path "$TEMP_DIR/roads.csv" \
  --max-events 1 --dry-run --json >"$TEMP_DIR/road-stream-preview.json"
python3 scripts/research/build_query_set.py \
  --dataset-manifest "$TEMP_DIR/roads.json" --output "$TEMP_DIR/road-queries.csv" \
  --manifest "$TEMP_DIR/road-queries.json" --queries 20 --seed 42 >/dev/null

python3 - "$TEMP_DIR/smartphone.json" "$TEMP_DIR/roads.json" "$TEMP_DIR/road-queries.json" \
    "$TEMP_DIR/road-stream-preview.json" "$TEMP_DIR/uniform.json" "$TEMP_DIR/gaussian.json" \
    "$TEMP_DIR/zipf.json" <<'PY'
import json, sys
smartphone, roads, queries = (json.load(open(path)) for path in sys.argv[1:4])
stream = json.load(open(sys.argv[4]))
synthetic = [json.load(open(path)) for path in sys.argv[5:]]
assert smartphone["validation"]["passed"]
assert roads["validation"]["passed"]
assert roads["projectedCrs"] == "EPSG:9678"
assert sum(item["objects"] for item in roads["partitionIndex"]) == roads["objects"]
assert queries["queries"] == 20 and queries["datasetCsvSha256"] == roads["csvSha256"]
sample = stream["sample"]["thesis/raw/csv"]
assert sample["appearanceProbability"] > 0 and sample["serverPartition"] >= 0
assert len(sample["mbrMin"]) == 2 and len(sample["mbrMax"]) == 2
assert {dataset["centerDistribution"] for dataset in synthetic} == {"uniform", "gaussian", "zipf"}
assert all(dataset["validation"]["passed"] and dataset["lmax"] == 8.0 for dataset in synthetic)
assert synthetic[2]["zipfSkewness"] == 0.8
print("paperDatasetsValidated=true smartphoneObjects=12 osmObjects=20 roadQueries=20 syntheticDistributions=3")
PY
