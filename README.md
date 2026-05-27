# Paper-Informed Apache Spark Top-k Dominance Task

This project is upgraded to use **Apache Spark** as the execution layer for probabilistic top-k ranking over uncertain and imperfect data. It follows the 2025 ICCIT PTD direction by keeping the core ideas — probabilistic repair, query-relative dynamic dominance, DSCP-style LB/UB pruning, AES-style compact emission, and exact refinement — while moving the runtime from Hadoop/Flink-style execution to Spark.

## Spark-first Architecture

```text
Raw uncertain events
  -> Spark parallel imputation into probabilistic instances
  -> deterministic object-to-server partition assignment
  -> aggregate R-tree per partition and selected exported-level LB/UB computation for MBR inputs
  -> selectable treatment: baseline | DSCP-only | AES-only | AES + DSCP
  -> optional DSCP-style per-partition kth-LB threshold pruning
  -> expanded or AES-aggregated partial-MBR-reference shuffle stage
  -> reducer traversal of partial MBRs and exact Spark refinement
  -> TopKResult per query
```

Curated uncertain-object records carrying MBR coordinates build one packed aggregate R-tree per
server partition. The engine selects an exported index level using a deterministic representative
probe sample to estimate exported-node, partial-reference and reducer-traversal work cost, uses fully
dominated node aggregates in bounds, and
traverses partial MBR references in the reducer-shaped Spark stage. Inputs without MBR metadata,
including generated raw stream events, use an index-free conservative remote-partition upper
allowance. Neither mode computes a global exact score during filtering.

For the finite streaming benchmark, ingress is handled by Spark Structured Streaming:

```text
Python simulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka
  -> Apache Spark Structured Streaming (AvailableNow bounded snapshot)
  -> SparkTopKEngine
  -> TopKResult
```

The bounded snapshot is intentional: each saved PTD benchmark evaluates one fixed set of
uncertain events. It avoids comparing algorithms against different moving stream contents.

Legacy Flink source files are retained only for comparison. The default build image, Docker Compose services, Kubernetes manifest, E2E scripts, validation scripts, and monitor now target Spark.

## Main Commands

```bash
just
just test
just spark
just csv-test
just ablation-test
just stream-test
just compare-runs csv-run-a csv-run-b
just web
just web-test
just web-smoke-test
just image
just setup
just spark-submit
just e2e
just validate
just monitor-bg
just test-all
```

Run Spark locally on generated data:

```bash
OBJECTS=1000 QUERIES=2 DIMENSIONS=4 K=10 PARTITIONS=4 just spark
```

Run the Spark entry point directly:

```bash
mvn -q -DskipTests compile exec:java \
  -Dexec.mainClass=com.thesis.topk.spark.ProbabilisticTopKSparkJob \
  -Dexec.args="--source=simulator --dataset=synthetic --objects=1000 --queries=2 --dimensions=4 --k=10 --partitions=4 --algorithm=aes-dscp --sparkMaster=local[*]"
```

## Docker Compose

Build the Spark runtime image:

```bash
just image
```

Start Kafka, EMQX, Spark master, and Spark worker:

```bash
just setup
```

Spark services exposed by `docker-compose.e2e.yml`:

- Spark master RPC: `spark://localhost:7077`
- Spark master UI: `http://localhost:8080`
- Spark worker UI: `http://localhost:8081`
- Kafka external listener: `localhost:29092`
- EMQX MQTT: `localhost:18884`
- EMQX dashboard/API: `http://localhost:18084`

Submit the bounded Kafka Spark job to the Compose Spark cluster:

```bash
EXPECTED_MESSAGES=400 OBJECTS=200 QUERIES=2 K=10 just spark-submit
```

Run the full Dockerized E2E benchmark:

```bash
OBJECTS=200 QUERIES=2 DIMENSIONS=4 K=10 MISSING_RATE=0.35 \
RATE_PER_SECOND=200 QOS=0 PARTITIONS=4 just e2e
```

E2E artifacts:

```text
reports/e2e/summary.md
reports/e2e/summary.csv
reports/e2e/spark.log
reports/e2e/spark-submit.log
```

The E2E profile now uses Spark's Kafka Structured Streaming source rather than a driver-side
Kafka consumer. It requires the `spark-sql-kafka-0-10_2.12` connector packaged with the app.

## Kubernetes

Build the image and make it available to your cluster, then apply the manifest:

```bash
just image
just simulator-image
just k8s-apply
just k8s-pods
just k8s-logs
```

The Kubernetes manifest creates:

- `Namespace/thesis-streaming`
- `StatefulSet/kafka`
- `Deployment/emqx`
- `Deployment/incomplete-data-publisher`
- `Deployment/spark-master`
- `Deployment/spark-worker`
- `Job/spark-topk-submit`

The Spark submit job consumes Kafka topics and submits to `spark://spark-master:7077`.

## Dataset Providers

Datasets are selected through the same CLI surface used by the synthetic generator:

```bash
DATASET=csv DATASET_PATH=/path/to/events.csv just spark
```

Built-in providers:

- `synthetic`: generated incomplete records.
- `csv`: generic normalized CSV records.
- `intel`: `datasets-raw/intel_lab_data.gz`.
- `pump`: `datasets-raw/pump_sensor_data.zip`.
- `gas`: `datasets-raw/gas+sensors+for+home+activity+monitoring.zip`.
- `all`: Intel, pump, and gas together, processed as separate query families.

### Paper Dataset Curation

The local `datasets-osm/` source material is intentionally not committed. Its reproducible
pre-curation contract is tracked in `config/research/bangladesh-osm-replication.json`.
Validate that the supplied HOTOSM/OpenStreetMap export is suitable for road-to-MBR curation:

```bash
just osm-prepare-check
```

The check verifies the OSM snapshot, attribution, bounding box, metric CRS and sufficient road
features. Generate validated paper artifacts with:

```bash
python3 scripts/research/build_paper_dataset.py smartphone \
  --output datasets-curated/smartphone-paper.csv \
  --manifest reports/datasets/smartphone-paper.json \
  --objects 750 --instances-min 8 --instances-max 20 --queries 20 --partitions 8 --seed 42
uv run --with pyproj scripts/research/build_paper_dataset.py osm \
  --output datasets-curated/bangladesh-road-paper.csv \
  --manifest reports/datasets/bangladesh-road-paper.json \
  --objects 98451 --instances-min 5 --instances-max 11 --queries 1 --partitions 8 --seed 42
python3 scripts/research/build_paper_dataset.py synthetic \
  --distribution zipf --zipf-skew 0.8 --lmax 10 \
  --output datasets-curated/rai-zipf.csv \
  --manifest reports/datasets/rai-zipf.json \
  --objects 100000 --instances-min 2 --instances-max 10 --queries 1 --partitions 8 --seed 42
```

The road generator converts line MBRs into EPSG:9678, uniformly samples 5-11 instances,
normalizes appearance probabilities, assigns each object to one seeded partition and records
a partition index manifest. The `synthetic` generator implements Rai-Lian square uncertain
regions with uniform, Gaussian or Zipf center distributions and configurable `lmax`. The CSV
provider consumes this evidence schema without discarding probabilities or server assignment.

CSV header format:

```text
objectId,queryId,eventTime,opType,a0,a1,a2,...
```

Missing values can be blank, `null`, `NaN`, or `?`. `opType` may be blank and defaults to `UPSERT`.
Paper-curated CSVs additionally supply `instanceId`, `probability`, `serverId`,
`queryA0...queryAn`, and MBR bounds.

For Docker/Kubernetes runs, `DATASET_PATH` must be visible inside the container. The Spark
runtime image is intentionally lean; local commands mount `datasets-raw/` at
`/opt/spark/datasets-raw` only for raw-provider runs rather than embedding large source
archives into each benchmark image.

## Research Test Profiles And Saved Runs

Run the deterministic CSV fixture through Spark and exactness validation:

```bash
RUN_ID=csv-baseline just csv-test
```

Run the controlled four-treatment ablation on one deterministic input setup:

```bash
RUN_ID=paper-ablation BUILD_IMAGE=0 just ablation-test
```

Paper-sized indexed road executions require an explicit driver heap budget because the local
Spark profile gathers distributed index summaries for broadcast:

```bash
PROFILE=road-full ALLOW_FULL_ROAD=true SPARK_DRIVER_MEMORY=8g \
  VALIDATE_EXACT=false RUN_ID=road-full just iccit-compare
```

Selectable algorithm IDs are `baseline`, `dscp-only`, `aes-only`, and `aes-dscp`.
Each saved query records the executed emitted-record count, baseline/AES emission counts,
`AER`, pruning ratio, exact-oracle `falsePrunes` audit, filtering/emission/refinement time,
observed Spark shuffle bytes/records, executor/GC time and task-straggler ratio.
Saved Spark runs record their bound mode: `rai-lian-artree-selected-level-partial-reducer` for curated MBR inputs and
`conservative-remote-mass-no-mbr` for raw events without spatial summaries. The validated
MBR ablation and road-smoke profiles demonstrate safe DSCP pruning and reducer traversal while
preserving exact output; these are regression proofs, not claims that the published Hadoop
percentages have been reproduced.

The fixture is `tests/fixtures/csv/smartphone-small.csv`. To exercise another normalized CSV:

```bash
RUN_ID=csv-custom CSV_PATH=/absolute/path/events.csv scripts/research/run_csv_benchmark.sh
```

Run the actual stream route with a finite reproducible workload:

```bash
RUN_ID=stream-baseline OBJECTS=12 QUERIES=2 DIMENSIONS=2 K=2 just stream-test
```

Validate the separate Hadoop/HDFS/YARN MapReduce execution surface:

```bash
just hadoop-smoke
```

This validates Hadoop infrastructure with an official MapReduce examples job only. It is not a
Hadoop PTD baseline and is intentionally excluded from Spark performance comparisons until
Rai-Lian/ICCIT Mapper/Reducer jobs are implemented over the same curated inputs.

To keep a command-line validation run from updating the latest shared E2E
snapshot files, direct its transient logs to another directory:

```bash
RUN_ID=stream-check E2E_REPORT_DIR=/tmp/ptd-stream-check \
  BUILD_IMAGE=0 tests/e2e/test_mqtt_kafka_spark.sh
python3 scripts/validate-e2e.py --report-dir /tmp/ptd-stream-check \
  --expected-messages 24 --expected-queries 2
```

`just test-all` uses isolated temporary output automatically while still
persisting immutable completed-run evidence under `reports/runs/<run-id>/`.

## PTD-BenchLab Website

Start the local research workbench:

```bash
just web
```

Open `http://127.0.0.1:8090`. Set another port with `WEB_PORT=8091 just web`.

The dependency-free website is backed directly by the saved experiment evidence in
`reports/runs/*`. It provides:

- a dashboard for validated CSV and MQTT/Kafka/Spark runs;
- click-through run details with configuration, per-query pruning, thresholds and logs;
- fair-comparison warnings when seeds, partitions, dataset hashes or other controls differ;
- CSV record inspection, probability/MBR audit and input-quality summaries;
- simulator controls and status manifests for smartphone and Bangladesh OSM road artifacts;
- launch controls for the validated CSV and bounded streaming benchmark profiles;
- exactness status, measured runtime/pruning/telemetry views, experiment matrix planning,
  LaTeX table export and downloadable evidence bundles.

The site can launch all four treatment variants and compare saved AER/pruning/shuffle evidence.
It does not claim the published reduction percentages until a controlled full-scale suite has
been executed and reviewed.

Validate both its artifact logic and its served HTTP endpoints from the terminal:

```bash
just web-test
just web-smoke-test
```

Every profile produces an immutable run directory under `reports/runs/<run-id>/`:

```text
manifest.json       configuration, dataset SHA-256, commit and dirty-worktree state
metrics.json        Spark/PTD metrics, algorithm time, validation time, correctness status
metrics.csv         query-level variant, emission, AER, pruning, phase, shuffle and validation export
spark.log           execution evidence
algorithm.log       CSV profile exact-vs-pruned validation evidence
e2e-summary.csv     stream profile ingress and elapsed-time evidence
dataset-manifest.json optional generated-dataset curation/validation evidence
```

Compare two saved setups:

```bash
just compare-runs csv-baseline csv-partitions-4
```

The comparison uses algorithm time excluding the optional oracle-check duration, and warns if
fairness-critical setup fields differ, including dataset checksum, data shape, `k`, partition
count, or seed.

## MQTT/Kafka Ingress

The simulator publishes normalized JSON records such as:

```json
{"objectId":"obj-1","queryId":"q0","eventTime":1700000000000,"opType":"UPSERT","attributes":[0.42,null,0.7],"missingMask":[false,true,false]}
```

With `DATASET=all`, each raw dataset is published to a separate MQTT topic and bridged to a separate Kafka topic:

```text
thesis/raw/intel -> thesis.raw.intel
thesis/raw/pump  -> thesis.raw.pump
thesis/raw/gas   -> thesis.raw.gas
```

Run a small all-dataset Spark E2E smoke test:

```bash
DATASET=all MAX_EVENTS=5 EXPECTED_MESSAGES=15 QUERIES=1 K=2 just e2e
```

## Monitoring and Validation

Start the monitor:

```bash
PORT=8088 just monitor
```

The monitor reads Spark logs and E2E summary files, then reports MQTT, Kafka, Spark TopKResult count, ingestion completeness, pruning metrics, and imputation MAE.

Validate the latest run:

```bash
EXPECTED_MESSAGES=400 QUERIES=2 just validate
```

Run full checks:

```bash
just test-all
```

## Reports

Spark upgrade report:

```text
reports/implementation/spark-upgrade-report.md
```

Paper alignment report:

```text
reports/validation/paper-alignment.md
```
Each new finite benchmark archives a bounded `object-traces.csv` sample containing per-object
LB/UB/tau decisions, partial MBR references and AES versus expanded emission counts. Once a
smartphone and road suite complete, render paper-shaped SVG evidence without mixing runtimes
from unlike machines:

```bash
just paper-figures <smartphone-suite-id> <road-suite-id>
```
