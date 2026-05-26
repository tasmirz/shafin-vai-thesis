A serious GUI for this paper should not be only a “run benchmark” button. It should be a **research workbench**: part data simulator frontend, part benchmark laboratory, part algorithm debugger, part reproducibility package builder, and part industry-grade observability dashboard. For this specific PTD paper, the GUI should expose the full pipeline: uncertain-object generation, query generation, Hadoop/MapReduce execution, DSCP/AES toggles, baseline comparison, raw intermediate inspection, correctness validation, ablation studies, and exportable academic results. The paper’s own experimental setup already gives the minimum baseline: synthetic smartphone data, Bangladesh road/OSM-derived uncertain spatial data, baseline vs proposed framework, wall-clock time, communication cost, AER, and AES/DSCP ablations; the GUI should make all of those configurable, inspectable, repeatable, and reportable. 

# Proposed GUI name

**PTD-BenchLab: Research GUI for Probabilistic Top-k Dominating Queries over Uncertain Databases**

A good subtitle would be:

**“A reproducible benchmarking, validation, simulation, and raw-inspection platform for uncertain-database dominance-query algorithms.”**

This name matters because it communicates that the system is not merely a frontend. It is a full experimental environment. In academic terms, it supports reproducibility, artifact evaluation, ablation, validation, and explainability. In industry terms, it supports observability, data quality, regression testing, run comparison, resource-cost accounting, and pipeline governance.

# High-level design philosophy

The GUI should be designed around five hard requirements.

First, it must make **every benchmark reproducible**. Every run should store the algorithm version, code commit, dataset version, simulator seed, query seed, Hadoop/Spark/runtime configuration, number of repetitions, warm-up policy, hardware metadata, OS/JVM/Python versions, and every input parameter. This follows the same spirit as experiment-tracking tools such as MLflow, which explicitly log parameters, metrics, artifacts, and code versions for later visualization and comparison. ([MLflow AI Platform][1])

Second, it must make **raw internals inspectable**. For this paper, that means not only “baseline took 66,520 ms and proposed took 43,757 ms,” but also: how many objects were pruned by DSCP, what was the τ threshold per partition, how many competitors were merged by AES, how many shuffle records were avoided, which MBRs were fully/partially dominated, and where the reducer spent time.

Third, it must make **correctness validation first-class**. PTD algorithms are dangerous to benchmark only by speed, because a faster method is useless if it silently changes the top-k set. The GUI should always pair performance with correctness: exact top-k agreement, dominance-score agreement, probability normalization checks, deterministic reruns, small-dataset brute-force oracle comparison, and output-cardinality checks.

Fourth, it must make **benchmarking scientifically disciplined**. The GUI should support repetitions, confidence intervals, warm/cold runs, variance reporting, parameter sweeps, ablation matrices, skew tests, stress tests, and failure logs. A tool such as BenchExec is useful inspiration because it focuses on reliable benchmarking, resource measurement, reproducibility, and result analysis for large benchmark sets. ([GitHub][2])

Fifth, it must make **industry deployment diagnosable**. In industry, people will ask: Why is this slow? Which partition is skewed? Which node is a straggler? What is the memory pressure? Did shuffle explode? Did input data quality degrade? This is why the GUI should use observability concepts: metrics, logs, and traces. OpenTelemetry is a good reference model because it treats metrics, traces, and logs as correlated telemetry signals. ([OpenTelemetry][3])

# Main GUI modules

## 1. Research project dashboard

The first screen should show a project-level summary.

It should contain:

| Panel                 | Purpose                                                                               |
| --------------------- | ------------------------------------------------------------------------------------- |
| Current project       | Paper name, experiment family, active branch, algorithm version                       |
| Datasets              | Synthetic smartphone, Bangladesh road, custom uploaded data, simulator-generated data |
| Algorithms            | Baseline distributed PTD, DSCP-only, AES-only, AES+DSCP, brute-force oracle           |
| Recent runs           | Run ID, status, dataset, k, dimensions, objects, runtime, correctness                 |
| Best result           | Fastest exact run, best speedup, best communication reduction                         |
| Reproducibility score | Whether the run has seed, config, code commit, environment, artifacts                 |
| Warnings              | Non-normalized probabilities, missing seeds, high variance, failed validation         |
| Export status         | Paper table ready, LaTeX plot ready, artifact bundle ready                            |

The dashboard should immediately tell the researcher: “What have I tested, what is valid, what is faster, and what still needs proof?”

For this paper specifically, the dashboard should have a card saying:

**Paper baseline reproduced?**

It should compare the user’s latest results against the paper’s reported numbers: synthetic baseline/proposed wall-clock time, real baseline/proposed wall-clock time, communication cost, AES-only, DSCP-only, and AES+DSCP ablation. The paper reports 34.2% synthetic reduction, 24.7% real-dataset reduction, and separate AES-only/DSCP-only ablation results; the GUI should turn these into target reproduction checks. 

## 2. Dataset simulator frontend

The GUI should be the frontend to the **DataSimulator**, not just a separate viewer. This is one of the most important design decisions. The simulator should be treated like a first-class scientific instrument.

The DataSimulator screen should have several modes.

### 2.1 Domain templates

The user should be able to choose a domain template:

| Template                                   | What it simulates                        | Attributes                                                          |
| ------------------------------------------ | ---------------------------------------- | ------------------------------------------------------------------- |
| Smartphone market                          | Similar to the paper’s synthetic dataset | price, battery, performance, camera quality, appearance probability |
| Road network / OSM spatial uncertainty     | Similar to Bangladesh road experiment    | longitude, latitude, road segment MBR, sampled uncertain instances  |
| IoT sensor network                         | Uncertain readings over time             | temperature, humidity, sensor error, timestamp, probability         |
| Recommendation system                      | Probabilistic user-item preference       | rating, price, distance, likelihood                                 |
| Financial portfolio                        | Risk-return uncertain tuples             | risk, return, volatility, probability                               |
| Generic multidimensional uncertain objects | Arbitrary benchmark data                 | d dimensions, object count, instances per object                    |

The paper used a synthetic smartphone dataset and a Bangladesh road dataset constructed from road geometries, where each road segment becomes an MBR and sampled instances are assigned normalized probabilities. The simulator should reproduce these modes directly and then generalize them. 

### 2.2 Simulator controls

The simulator should expose every data-generation parameter.

Core controls:

| Parameter                | Description                                                           |
| ------------------------ | --------------------------------------------------------------------- |
| Number of objects `N`    | Total uncertain objects                                               |
| Instances per object `m` | Fixed, range, Poisson, uniform, power-law                             |
| Dimensionality `d`       | 2D, 3D, 5D, 10D, high-dimensional stress mode                         |
| Probability model        | Uniform, Dirichlet-normalized, Gaussian noise, empirical distribution |
| Correlation type         | Independent, correlated, anti-correlated, clustered                   |
| Attribute distribution   | Uniform, normal, log-normal, Pareto, Zipf, spatial clustered          |
| Outlier percentage       | Example: 5% extreme objects, as in the paper’s synthetic dataset      |
| Missingness              | None, MCAR, MAR, MNAR, incomplete-data mode                           |
| Noise level              | Coordinate noise, measurement error, sensor error                     |
| Object skew              | Balanced, few heavy objects, power-law object size                    |
| Partition skew           | Balanced partitions, hash skew, spatial skew                          |
| Query count              | Number of random query points                                         |
| Query distribution       | Uniform, hotspot, near skyline, near dense cluster, road-specific     |
| Random seed              | Required for reproducibility                                          |
| Dataset ID               | Versioned dataset artifact name                                       |
| Export format            | CSV, JSONL, Parquet, HDFS-ready text, GeoJSON                         |

The GUI should not hide these in advanced config files. They should be visible, because benchmark credibility depends on them.

### 2.3 Probability editor

Uncertain databases rely on probabilities, so the GUI should have a probability-audit panel.

It should show:

| Check                           | Example                                                                              |                |
| ------------------------------- | ------------------------------------------------------------------------------------ | -------------- |
| Sum of probabilities per object | `Σ p(instance                                                                        | object) = 1.0` |
| Invalid probabilities           | negative, zero when not allowed, greater than one                                    |                |
| Floating-point drift            | 0.9999998 or 1.0000003                                                               |                |
| Normalization policy            | reject, auto-normalize, warn only                                                    |                |
| Probability histogram           | shows whether most instances are equally likely or skewed                            |                |
| Entropy per object              | high entropy means many plausible instances; low entropy means one dominant instance |                |

This is crucial because a benchmark can be invalid if probabilities are malformed.

### 2.4 Spatial simulator for road data

For the Bangladesh-road/OSM-like dataset, the GUI should include a map-based simulator.

It should allow:

| Feature                      | Details                                       |
| ---------------------------- | --------------------------------------------- |
| Import OSM/GeoJSON/Shapefile | Road geometries, administrative boundaries    |
| Select region                | Bounding box, polygon, city/district selector |
| Segment-to-MBR conversion    | Show road segment and its MBR                 |
| Instance sampling            | 5–11 samples per road segment, configurable   |
| Spatial uncertainty radius   | meters around road geometry                   |
| Road-type filters            | highway, residential, service road, rural     |
| Coordinate system            | WGS84, projected local CRS                    |
| Map overlay                  | roads, MBRs, sampled instances, query point   |
| Export                       | uncertain object table plus geometry metadata |

This would make the real-data experiment visually defensible. Reviewers could see exactly how road uncertainty was generated.

## 3. Raw dataset inspection

The raw-inspection feature should be one of the strongest parts of the GUI. A benchmark GUI without raw inspection becomes a black box.

The raw inspector should have multiple views.

### 3.1 Object-instance table

A table should show uncertain objects and their instances:

| object_id | instance_id | dim_1 | dim_2 |  … | probability | partition | source    |
| --------- | ----------- | ----: | ----: | -: | ----------: | --------: | --------- |
| Canon     | Canon1      |   750 |     9 |  … |        0.60 |         3 | synthetic |
| Canon     | Canon2      |   700 |     7 |  … |        0.40 |         3 | synthetic |

Required table features:

Search, filter, sort, group by object, group by partition, show probability sums, show invalid rows, export selected rows, freeze columns, compare two dataset versions, show generated vs imported source.

### 3.2 Object tree view

The GUI should show:

```text
Object Canon
  ├── Instance Canon1: (price=750, quality=9), p=0.60
  └── Instance Canon2: (price=700, quality=7), p=0.40
```

This is more readable than a flat table when inspecting uncertain objects.

### 3.3 Data profile view

The GUI should show:

| Profile                     | Why it matters                                                 |
| --------------------------- | -------------------------------------------------------------- |
| Object count                | Basic benchmark scale                                          |
| Instance count              | True computational scale                                       |
| Average instances/object    | Affects domination comparisons                                 |
| Dimensions                  | Higher dimensions change dominance behavior                    |
| Probability entropy         | Shows uncertainty level                                        |
| Attribute histograms        | Reveals distribution shape                                     |
| Correlation heatmap         | Dominance behavior differs for correlated/anti-correlated data |
| Outlier list                | Outliers can dominate or distort top-k                         |
| Partition size distribution | Reveals load imbalance                                         |
| MBR area distribution       | For spatial uncertain data                                     |
| Duplicate points            | Can affect dominance equality cases                            |
| Null/missing values         | Must be rejected or handled                                    |

Great Expectations is useful inspiration here because it supports human-readable data documentation generated from expectations and validation results. ([Great Expectations][4])

## 4. Visual dominance inspector

This is where the GUI becomes academically powerful.

For 2D and 3D cases, the GUI should visualize:

| Visualization            | Purpose                                   |
| ------------------------ | ----------------------------------------- |
| Query point `q`          | Center of dynamic dominance comparison    |
| DDR quadrants            | TL, TR, BL, BR dynamic dominance regions  |
| Object instances         | Colored by object                         |
| MBRs                     | Bounding rectangles for uncertain objects |
| Fully dominated MBRs     | Green or checked                          |
| Partially dominated MBRs | Yellow or hatched                         |
| Non-dominated MBRs       | Gray                                      |
| LB/UB labels             | Show per-instance and per-object bounds   |
| τ threshold              | Show current k-th largest LB frontier     |
| Pruned candidates        | Crossed out or faded                      |
| Surviving candidates     | Highlighted                               |
| Top-k final objects      | Emphasized                                |

The paper already uses DDR and MBR visualizations to explain dominance and partial dominance, so the GUI should turn those static figures into interactive tools. 

The user should be able to click an object and see:

```text
Object: Nikon
Instances: 2
ObjectLB: 3.41
ObjectUB: 5.82
Partition: 7
Status: Survived DSCP
Reason: UB(Nikon)=5.82 >= τ=4.90
Competitor set after AES: {Canon, Sony, Xiaomi, ...}
```

For a pruned object:

```text
Object: Oppo
ObjectLB: 1.12
ObjectUB: 2.48
τ: 3.05
Status: Pruned by DSCP
Reason: UB(Oppo) < τ
```

This is exactly the kind of raw interpretability that academic reviewers and industry engineers both want.

## 5. Algorithm registry

The GUI should have an algorithm registry where every algorithm is a selectable, versioned plugin.

Minimum algorithms:

| Algorithm                | Purpose                         |
| ------------------------ | ------------------------------- |
| Brute-force exact PTD    | Small-data correctness oracle   |
| Baseline distributed PTD | Paper baseline                  |
| Proposed AES+DSCP        | Full proposed method            |
| AES-only                 | Ablation                        |
| DSCP-only                | Ablation                        |
| No-pruning baseline      | Worst-case reference            |
| Single-machine PTD       | Local reference                 |
| Spark implementation     | Future extension                |
| Approximate PTD          | Optional industry-speed variant |
| Streaming PTD            | Future dynamic-data mode        |

The paper’s ablation study compares AES-only, DSCP-only, and AES+DSCP, so the GUI must expose these as explicit algorithm variants instead of hidden flags. 

Each algorithm entry should have:

| Field              | Example                                                |
| ------------------ | ------------------------------------------------------ |
| Algorithm name     | Proposed AES+DSCP                                      |
| Version            | `v1.2.0`                                               |
| Code commit        | Git SHA                                                |
| Language           | Java MapReduce                                         |
| Runtime            | Hadoop 3.x                                             |
| Exactness          | Exact                                                  |
| Input format       | uncertain-object table                                 |
| Output format      | top-k object list                                      |
| Supports ablation? | yes                                                    |
| Supports counters? | yes                                                    |
| Parameters         | `k`, partitions, reducers, pruning mode, emission mode |
| Paper mapping      | Section IV, Algorithm 1                                |
| Known limitations  | high-dimensional visualization limited                 |

## 6. Pipeline builder

The GUI should show the algorithm as a visual pipeline.

For this paper:

```text
Dataset
  ↓
Partitioning / InputSplit
  ↓
FilteringMapper
  ├── DDR tests
  ├── LB/UB computation
  ├── τ calculation
  ├── DSCP pruning
  └── AES emission
  ↓
Shuffle / Sort
  ↓
FilteringReducer
  ├── Aggregate LB/UB
  ├── Merge competitor sets
  └── Candidate filtering
  ↓
RefinementMapper
  ├── Re-score candidates
  ├── Validate dominance relations
  └── Produce final top-k
  ↓
Result / Report
```

Hadoop’s MapReduce model already separates input splitting, map tasks, sorted map outputs, reduce tasks, scheduling, monitoring, and re-execution of failed tasks; the GUI should mirror these ├── DDR tests
├── LB/UB computation
├── τ calculation
├── DSCP pruni([Apache Hadoop][5])↓
Shuffle / Sort
↓
FilteringReducer
├── Aggregate LB/UB
├── Merge competitor sets
└── Candidate filtering
↓
RefinementMapper
├── Re-score candidates
├── Validate dominance relations
└── Produce final top-k
↓
Result / Report

````

Hadoop’s MapReduce model already separates input splitting, map tasks, sorted map outputs, reduce tasks, scheduling, monitoring, and re-execution of failed tasks; execution stages rather than treating the job as a single opaque process. citeturn150364search0

The pipeline view should be interactive. Clicking **FilteringMapper** should reveal mapper-level records, counters, emissions, partition load, and DSCP decisions. Clicking **Shuffle** should reveal bytes, records, compression ratio, and skew. Clicking **RefinementMapper** should reveal final candidate rescoring.

## 7. Benchmark designer

This should be the heart of the system.

The benchmark designer should let users create experiments with full control over every factor.

### 7.1 Benchmark types

| Benchmark type | Purpose |
|---|---|
| Baseline comparison | Baseline vs proposed |
| Ablation study | AES-only, DSCP-only, AES+DSCP |
| Scalability test | Increase objects, instances, dimensions, partitions |
| Sensitivity test | Vary one parameter at a time |
| Stress test | Extreme skew, high dimensions, huge instance counts |
| Robustness test | Outliers, noisy probabilities, missing values |
| Reproducibility test | Repeat same run with same seed |
| Regression test | Compare current code against previous commit |
| Resource-limit test | CPU/memory/network constraints |
| Streaming simulation | Sliding-window or dynamic uncertain data |
| Cross-engine test | Hadoop vs Spark vs local |

### 7.2 Experiment matrix builder

The GUI should allow the user to define parameter sweeps.

Example:

| Parameter | Values |
|---|---|
| Dataset | synthetic-smartphone |
| `N_objects` | 1k, 5k, 10k, 50k |
| instances/object | 5, 10, 20 |
| dimensions | 2, 3, 5 |
| `k` | 5, 10, 20, 50 |
| partitions | 4, 8, 16, 32 |
| reducers | 1, 2, 4, 8 |
| algorithms | baseline, AES-only, DSCP-only, AES+DSCP |
| repetitions | 20 |
| seeds | fixed list |
| cache mode | cold, warm |
| cluster mode | pseudo-distributed, distributed |

This should generate a run count preview:

```text
4 object scales × 3 instance settings × 3 dimensions × 4 k values ×
4 partition settings × 4 algorithms × 20 repetitions
= 46,080 runs
Estimated time: high
Suggested reduction: use fractional factorial design
````

The GUI should warn researchers when they accidentally create a benchmark too large to finish.

### 7.3 Academic experiment templates

The GUI should provide prebuilt templates:

| Template                    | Output                                                  |                                   |
| --------------------------- | ------------------------------------------------------- | --------------------------------- |
| Paper reproduction          | Recreates paper tables and figures                      |                                   |
| Ablation only               | AES vs DSCP vs AES+DSCP                                 |                                   |
| Scalability by object count | Runtime vs number of objects                            |                                   |
| Scalability by dimensions   | Runtime vs dimensionality                               |                                   |
| Effect of `k`               | Runtime and candidate count vs `k`                      |                                   |
| Effect of uncertainty       | Runtime vs instances per object                         |                                   |
| Effect of partition count   | Runtime, skew, and shuffle vs par([ACM][6])n-cost study | Records, bytes, AER, shuffle size |
| Correctness validation      | Top-k equality against brute force                      |                                   |
| Reviewer artifact check     | Minimal deterministic workflow                          |                                   |

The ACM artifact-review criteria emphasize artifacts being documented, consistent, complete, exercisable, and supported by evidence of verification and validation. A GUI intended for academic use should directly support those requirements through reviewer-friendly workflows and one-click artifact bundles. citeturn278738search0

## 8. Benchmark execution control

The GUI should have an execution console like a mini cluster manager.

### 8.1 Run launcher

Fields:

| Field              | Example                                    |
| ------------------ | ------------------------------------------ |
| Run name           | `synthetic_10k_k20_AES_DSCP_seed42`        |
| Dataset            | `smartphone_v3_seed42`                     |
| Algorithm          | AES+DSCP                                   |
| Query set          | `queries_uniform_20_seed99`                |
| Runtime            | Hadoop pseudo-distributed                  |
| Repetitions        | 20                                         |
| Warm-up runs       | 3                                          |
| Timeout            | 30 minutes                                 |
| Memory limit       | 8 GB                                       |
| CPU pinning        | optional                                   |
| JVM options        | `-Xmx4g`                                   |
| Save intermediates | yes/no                                     |
| Validation mode    | brute-force if small, consistency if large |
| Artifact mode      | save all logs, configs, outputs            |

### 8.2 Queue view

The run queue should show:

| Column            | Meaning                                       |
| ----------------- | --------------------------------------------- |
| Run ID            | Unique run identifier                         |
| Status            | queued, running, failed, completed, validated |
| Algorithm         | baseline, AES-only, DSCP-only, full           |
| Dataset           | dataset ID                                    |
| Progress          | mapper %, reducer %, refinement %             |
| Current phase     | filtering, shuffle, reduce, refinement        |
| Runtime so far    | wall time                                     |
| Node              | execution node                                |
| Failure reason    | if failed                                     |
| Validation status | pending/pass/fail                             |

### 8.3 Live execution monitor

During a run, show:

| Metric                                                   | Why it matters                |
| -------------------------------------------------------- | ----------------------------- |
| Wall-clock time                                          | Main paper metric             |
| CPU utilization                                          | Detect underuse or saturation |
| Memory usage                                             | Detect spills, GC, OOM risk   |
| Disk read/write                                          | HDFS/local I/O pressure       |
| Network throughput([Apache Hadoop][7])Map output records | Emission overhead             |
| Reduce input records                                     | Shuffle burden                |
| Spilled records                                          | Memory pressure               |
| Failed tasks                                             | Cluster instability           |
| Straggler tasks                                          | Partition skew                |
| GC time                                                  | JVM overhead                  |
| Custom counters                                          | DSCP/AES internals            |

Hadoop counters are especially important because MapReduce applications can report global statistics through framework and custom counters; mapper and reducer implementations can use counters to report job statistics. citeturn150364search3

## 9. PTD-specific metric system

The GUI should not only collect generic system metrics. It must collect PTD-specific metrics.

### 9.1 Core paper metrics

| Metric             | Meaning                             |
| ------------------ | ----------------------------------- |
| `act_WC`           | Actual wall-clock time              |
| `T_filter`         | Filtering phase time                |
| `T_refine`         | Refinement phase time               |
| `act_CC`           | Communication cost                  |
| `AER`              | Aggregated Emission Rate            |
| Speedup            | baseline/proposed runtime           |
|                    | reported improvement                |
| Output cardinality | should match expected top-k         |
| Accuracy/exactness | top-k match against oracle/baseline |

The paper defines wall-clock time as filtering plus refinement time, averages over 20 random queries, defines communication cost using candidate instances and partially dominated object lists, and defines AER as AES emissions divided by baseline emissions. The GUI should implement these formulas directly and show the formula beside each metric. fileciteturn4file0

### 9.2 DSCP metrics

| Metric                   | Description                         |
| ------------------------ | ----------------------------------- |
| `tau_per_partition`      | k-th largest LB threshold           |
| `objects_before_DSCP`    | objects considered                  |
| `objects_after_DSCP`     | objects surviving                   |
| `instances_before_DSCP`  | instance count before pruning       |
| `instances_after_DSCP`   | instance count after pruning        |
| `pruned_object_count`    | number pruned                       |
| `pruned_instance_count`  | instances removed                   |
| `pruning_ratio`          | pruned / total                      |
| `false_prune_count`      | should always be zero in exact mode |
| `UB_less_than_tau_count` | DSCP trigger count                  |
| `tau_updates`            | how often threshold changed         |
| `LB_distribution`        | histogram                           |
| `UB_distribution`        | histogram                           |
| `UB_minus_tau_margin`    | safety margin                       |
| `borderline_candidates`  | candidates close to threshold       |

The GUI should allow clicking any pruned object and seeing the exact inequality:

```text
ObjectUB[o] < τ
2.48 < 3.05
Therefore object o cannot enter top-k under current bound.
```

### 9.3 AES metrics

| Metric                         | Description                           |                                    |
| ------------------------------ | ------------------------------------- | ---------------------------------- |
| `baseline_emissions`           | number of records baseline would emit |                                    |
| `aes_emissions`                | number of aggregated records          |                                    |
| `competitors_per_instance_avg` | average merged competitor count       |                                    |
| `competitors_per_instance_max` | worst-case competitor set size        |                                    |
| `shuffle_record_reduction`     | record count reduction                |                                    |
| `shuffle_byte_reduction`       | byte reduction, if measuretio`        | baseline emissions / AES emissions |
| `avg_aggregated_record_size`   | compact tuple size                    |                                    |
| `decode_time`                  | reducer cost of unpacking AES record  |                                    |
| `reducer_input_records`        | actual records entering reducer       |                                    |
| `network_bytes`                | observed shuffle/network bytes        |                                    |

The paper says AES groups competitor IDs into one compact record per surviving instance using a format like `<objectId, instanceId, eid1;...;eidN, lb, ub, τ>`. The GUI should decode this format and show it visually. fileciteturn4file0

### 9.4 Dominance and geometry metrics

| Metric                         | Description                        |
| ------------------------------ | ---------------------------------- |
| `DDR_tests`                    | number of dynamic dominance checks |
| `MBR_full_dominance_count`     | fully contained MBRs               |
| `MBR_partial_dominance_count`  | partially overlapping MBRs         |
| `MBR_non_dominance_count`      | non-overlapping MBRs               |
| `same_object_dominance_count`  | intra-object contribution          |
| `cross_object_dominance_count` | inter-object contribution          |
| `dominance_graph_edges`        | if graph view is enabled           |
| `candidate_density`            | candidates per region              |
| `query_region_density`         | data density near query point      |

## 10. Validation center

A research GUI must have a **Validation Center** separate from benchmark charts.

Performance answers the question: “How fast?”

Validation answers: “Is it correct?”

### 10.1 Correctness checks

| Check                           | Purpose                                    |
| ------------------------------- | ------------------------------------------ |
| Brute-force oracle              | For small datasets, compare exact top-k    |
| Baseline agreement              | Proposed output equals baseline output     |
| Top-k set equality              | Same object IDs                            |
| Top-k order equality            | Same ranking                               |
| Score equality                  | Same domination probabilities or bounds    |
| Output cardinality              | exactly `k`, unless fewer valid objects    |
| Probability normalization       | each object sums to one                    |
| Determinism check               | same seed gives same result                |
| Monotonic bound check           | `LB <= UB` always                          |
| DSCP safety check               | no pruned object can beat τ                |
| AES losslessness check          | decoded competitor set equals original set |
| MBR consistency                 | MBR contains all object instances          |
| Partition consistency           | no object lost during partitioning         |
| Reducer aggregation consistency | sum of partials equals global value        |

### 10.2 Validation result view

Each run should have a validation badge:

| Badge                 | Meaning                             |
| --------------------- | ----------------------------------- |
| ✅ Exact               | Matches brute-force oracle          |
| ✅ Baseline-equivalent | Matches trusted baseline            |
| ⚠ Approximate         | Output differs but within tolerance |
| ❌ Incorrect           | Top-k mismatch                      |
| ❌ Invalid data        | Probability/schema issue            |
| ❌ Non-deterministic   | Same seed produced different result |
| ⚠ Not validated       | Too large; no oracle run            |

### 10.3 Difference inspector

If proposed and baseline differ, the GUI should show:

```text
Top-k mismatch detected

Baseline:
1. A, score=...
2. B, score=...
3. C, score=...

Proposed:
1. A, score=...
2. C, score=...
3. D, score=...

Missing from proposed: B
Extra in proposed: D

Possible cause:
- Object B was pruned by DSCP in partition 4.
- UB(B)=12.3, τ=12.7.
- Recheck bound calculation.
```

This kind of diagnosis is far more useful than simply saying “validation failed.”

## 11. Raw intermediate-data inspection

This is the “nitty gritty” part. The GUI should allow researchers to inspect intermediate MapReduce artifacts.

### 11.1 Mapper inspection

For each mapper:

| Field                         | Example                  |
| ----------------------------- | ------------------------ |
| Mapper ID                     | `map_003`                |
| Input split                   | HDFS path and byte range |
| Object count                  | 12,500                   |
| Instance count                | 126,000                  |
| Partition IDs                 | 0–15                     |
| DDR tests                     | 8.2M                     |
| LB/UB computed                | yes                      |
| DSCP pruned                   | 3,412 objects            |
| AES emitted                   | 42,100 records           |
| Baseline-equivalent emissions | 2.1M records             |
| Runtime                       | 4.2 s                    |
| Peak memory                   | 1.1 GB                   |

The user should be able to click **View sample records**:

```text
Input record:
object_id=Canon, instance_id=Canon1, attrs=[750, 9], p=0.6

Computed:
LB=...
UB=...
partition=3
status=survived

AES output:
key=3
value=<Canon, Canon1, Nikon;Sony;Xiaomi;..., lb=..., ub=..., tau=...>
```

### 11.2 Shuffle inspection

Shuffle view should show:

| Metric              | Why                         |
| ------------------- | --------------------------- |
| Map output records  | Directly affected by AES    |
| Map output bytes    | Actual network burden       |
| Compressed bytes    | If compression enabled      |
| Reduce input groups | Reducer workload            |
| Skew by reducer     | Detect overloaded reducers  |
| Largest key group   | Shows partition/object skew |
| Spill count         | Memory pressure             |
| Sort time           | Hidden overhead             |

### 11.3 Reducer inspection

Reducer view:

| Field                      | Example      |
| -------------------------- | ------------ |
| Reducer ID                 | `reduce_001` |
| Input partitions           | 0, 1, 2      |
| Candidate objects          | 8,200        |
| Aggregated competitor sets | 8,200        |
| LB/UB aggregation time     | 1.8 s        |
| Candidate filtering time   | 0.7 s        |
| Output candidates          | 210          |
| Straggler status           | no           |

### 11.4 Refinement inspection

Refinement view:

| Field                       | Example    |
| --------------------------- | ---------- |
| Candidate count             | 210        |
| Exact rescoring comparisons | 44,100     |
| Final top-k                 | 10 objects |
| Time                        | 2.2 s      |
| Validation status           | pass       |

## 12. Result analytics

The result-analysis module should produce both academic and industry views.

### 12.1 Academic chart types

| Chart                               | Purpose                             |
| ----------------------------------- | ----------------------------------- |
| Bar chart: baseline vs proposed     | Reproduce paper-style result        |
| Line chart: runtime vs object count | Scalability                         |
| Line chart: runtime vs dimension    | High-dimensional behavior           |
| Box plot: repeated run distribution | Variance                            |
| Error bars: mean ± CI               | Statistical reliability             |
| Stacked time bar                    | Filtering vs refinement             |
| Ablation grouped bar                | AES-only, DSCP-only, full           |
| Heatmap                             | runtime over `k` and partitions     |
| Scatter plot                        | communication cost vs runtime       |
| Pareto frontier                     | speed vs correctness/resource       |
| Partition skew plot                 | max/median reducer load             |
| CDF                                 | latency distribution across queries |

### 12.2 Industry chart types

| Chart                 | Purpose                             |
| --------------------- | ----------------------------------- |
| Time-series dashboard | Runtime over builds                 |
| Regression chart      | Did new commit slow down?           |
| SLA panel             | p50, p95, p99 latency               |
| Cost panel            | CPU-hours, memory-hours, network GB |
| Failure dashboard     | failed runs, timeout rate           |
| Cluster utilization   | are resources wasted?               |
| Data-quality trend    | invalid probability rate over time  |
| Alert timeline        | when performance regressed          |

### 12.3 Statistical reporting

The GUI should compute:

| Statistic                | Use                                      |
| ------------------------ | ---------------------------------------- |
| Mean                     | Standard paper result                    |
| Median                   | Robust to outliers                       |
| Standard deviation       | Variability                              |
| Coefficient of variation | Stability                                |
| Confidence interval      | Academic reliability                     |
| Min/max                  | Operational extremes                     |
| p95/p99                  | Industry latency                         |
| Speedup                  | baseline / proposed                      |
| Reduction percentage     | `(baseline - proposed) / baseline × 100` |
| Effect size              | practical significance                   |
| Paired test              | when same query seeds are used           |

## 13. Experiment tracking and artifact store

Every run should be stored like a research artifact.

A run record should contain:

```yaml
run_id: synthetic_10k_k20_full_seed42_2026_05_26
project: PTD-BenchLab
paper: distributed_ptd_dscp_aes
algorithm:
  name: AES+DSCP
  version: 1.2.0
  commit: abc123
dataset:
  id: smartphone_v3_seed42
  generator: DataSimulator
  seed: 42
  objects: 10000
  instances_per_o:contentReference[oaicite:14]{index=14}k: 20
  query_seed: 99
runtime:
  engine: Hadoop
  mode: pseudo_distributed
  java: 1.8
  os: Windows 11
metrics:
  act_WC_ms: 43757
  T_filter_ms: ...
  T_refine_ms: ...
  act_CC: ...
  AER: ...
validation:
  status: pass
  oracle: baseline
artifacts:
  config: config.yaml
  logs: logs/
  output: topk.csv
  plots: plots/
```

DVC is a good reference for versioning data, models, and experiments in a Git-like workflow, especially when datasets and code evolve at different speeds. citeturn618207search3

## 14. Reproducibility package builder

The GUI should have a button:

**Export Reproducibility Bundle**

The bundle should include:

| Artifact                                 | Contents                       |
| ---------------------------------------- | ------------------------------ |
| `README.md`                              | How to reproduce               |
| `environment.yml` or Dockerfile          | Environment                    |
| `config.yaml`                            | Benchmark parameters           |
| `dataset_manifest.json`                  | Dataset versions and checksums |
| `queries.json`                           | Query points and seeds         |
| `run.sh`                                 | One-command reproduction       |
| `results.csv`                            | Raw results                    |
| `summary.md`                             | Human-readable summary         |
| `plots/`                                 | Figures                        |
| `logs/`                                  | Execution logs                 |
| `counters.json`                          | Hadoop/custom counters         |
| `validation.json`                        | Correctness checks             |
| `paper_tables.tex`                       | LaTeX tables                   |
| `ar([ACM SIGSIM][8])les and descriptions |                                |

A reviewer mode should show:

```text
Can this artifact be evaluated?

✅ Dataset included or generator included
✅ Seeds included
✅ Code commit included
✅ Environment specified
✅ Run command provided
✅ Expected outputs provided
✅ Validation script provided
✅ Results match paper table within tolerance
```

This maps well to ACM-style artifact evaluation, where artifacts are judged by availability, functionality, reusability, and reproducibility of results. citeturn278738search32

## 15. Data quality and validation rules

The GUI should allow the user to define “expectations” for benchmark data.

Example rules:

```yaml
expectations:
  - each_object_probability_sum_between:
      min: 0.999999
      max: 1.000001
  - no_negative_probabilities: true
  - no_nan_attributes: true
  - instance_count_per_object_between:
      min: 1
      max: 100
  - mbr_contains_all_instances: true
  - object_id_unique: true
  - instance_id_unique_within_object: true
  - dimension_count_equals: d
```

Data validation is not optional. If a dataset has broken probabilities, the benchmark result is not meaningful.

The GUI should show validation output like:

```text
Dataset validation failed

12 objects have probability sum != 1
3 instances have negative probability
1 road segment has MBR that does not contain all sampled points

Action:
- reject dataset
- auto-normalize probabilities
- export invalid rows
```

## 16. Query generator frontend

The GUI should include a query generator separate from the dataset generator.

Query parameters:

| Parameter            | Options                                                  |
| -------------------- | -------------------------------------------------------- |
| Number of queries    | 1, 10, 20, 100                                           |
| Query distribution   | uniform, clustered, hotspot, boundary, near dense region |
| `k` values           | fixed or sweep                                           |
| Query dimensionality | must match dataset                                       |
| Query seed           | fixed                                                    |
| Domain-aware query   | road map click, smartphone preference point              |
| Save query set       | yes                                                      |
| Query replay         | rerun exact same query set                               |

For road/spatial data, the user should be able to click on a map and create a query point.

For smartphone data, the user should be able to define something like:

```text
Query preference:
price = 700
battery = 5000
camera = 8.5
```

Then the GUI can show which objects dynamically dominate others relative to that query.

## 17. Configuration diff viewer

Academic and industry users both need to know why two runs differ.

The GUI should have a side-by-side config diff:

| Field              | Run A    | Run B    |
| ------------------ | -------- | -------- |
| Algorithm          | Baseline | AES+DSCP |
| Dataset seed       | 42       | 42       |
| Query seed         | 99       | 99       |
| k                  | 20       | 20       |
| Partitions         | 16       | 16       |
| Reducers           | 4        | 4        |
| JVM heap           | 4 GB     | 4 GB     |
| DSCP               | off      | on       |
| AES                | off      | on       |
| Save intermediates | yes      | yes      |

The GUI should warn when a comparison is unfair:

```text
Warning: These runs use different query seeds.
Speedup may not be comparable.
```

or:

```text
Warning: Baseline used 4 reducers, proposed used 8 reducers.
This is not a fair algorithmic comparison.
```

This is essential. A benchmarking GUI must protect users from accidental bad science.

## 18. Fairness and benchmark hygiene

The GUI should include a **Benchmark Hygiene Checklist**.

Before running, it should ask:

| Check                         | Status |
| ----------------------------- | ------ |
| Same dataset?                 | yes    |
| Same query set?               | yes    |
| Same hardware?                | yes    |
| Same numb([Oracle][9])        |        |
| Same runtime engine?          | yes    |
| Same JVM options?             | yes    |
| Same partition/reducer count? | yes    |
| Warm-up policy declared?      | yes    |
| Random seeds fixed?           | yes    |
| Validation enabled?           | yes    |
| Raw logs saved?               | yes    |

For Java implementations, the GUI should also support warm-up configuration. JMH is relevant inspiration here because Java microbenchmarking must account for JVM warm-up and optimization effects, even though this PTD benchmark is larger than a microbenchmark. citeturn278738search34

## 19. Benchmark protocol editor

A very strong academic feature would be a protocol editor.

The researcher writes:

```markdown
Hypothesis H1:
AES reduces shuffle record count and wall-clock time compared with baseline.

Hypothesis H2:
DSCP reduces candidate count before refinement without changing exact top-k output.

Hypothesis H3:
AES contributes more than DSCP on synthetic smartphone data, while DSCP contributes more under high candidate skew.

Independent variables:
- algorithm variant
- dataset size
- k
- dimensionality
- instances per object
- partition count

Dependent variables:
- act_WC
- act_CC
- AER
- candidate count
- pruning ratio
- correctness

Controls:
- same hardware
- same query seed
- same dataset seed
- same JVM config
```

The GUI should connect this protocol to actual benchmark runs. When the benchmark finishes, it should say which hypotheses are supported.

## 20. Industry regression testing

In industry, this GUI should connect to CI/CD.

Example workflow:

```text
Developer opens pull request
  ↓
CI generates benchmark dataset
  ↓
Runs small benchmark suite
  ↓
Compares runtime and correctness against main branch
  ↓
Fails PR if:
    - top-k output changes unexpectedly
    - runtime regresses by >10%
    - memory increases by >20%
    - validation fails
```

Regression rules:

| Rule              | Example                                  |
| ----------------- | ---------------------------------------- |
| Correctness gate  | Proposed top-k must match baseline       |
| Runtime gate      | No more than 10% slower                  |
| Memory gate       | No more than 20% higher peak memory      |
| Shuffle gate      | No more than 15% more shuffle bytes      |
| Stability gate    | coefficient of variation below threshold |
| Data-quality gate | zero invalid probability rows            |

This transforms the GUI from an academic demo into a real engineering tool.

## 21. Observability and tracing

Each benchmark run should have a trace.

A trace m([OpenTelemetry][3])
Run trace
├── load_dataset: 1.2s
├── partition_data: 0.8s
├── filtering_mapper: 18.4s
│   ├── DDR_tests: 12.0s
│   ├── LB_UB_compute: 4.1s
│   ├── DSCP_prune: 1.0s
│   └── AES_emit: 1.3s
├── shuffle: 9.8s
├── reducer_aggregation: 7.4s
├── refinement_mapper: 5.2s
└── validation: 1.1s

````

This helps answer: “Where did the time go?”

The GUI should correlate traces with metrics and logs. OpenTelemetry’s model of traces, metrics, and logs provides a good industry pattern for this. citeturn618207search1

## 22. Raw log viewer

The GUI should have a searchable log view.

Features:

| Feature | Purpose |
|---|---|
| Filter by phase | mapper, reducer, refinement |
| Filter by severity | info, warning, error |
| Search object ID | debug one object |
| Search partition ID | debug skew |
| Download logs | artifact packaging |
| Link logs to timeline | find cause of slowdown |
| Structured logs | JSON preferred |
| Redaction | hide sensitive paths/data |

Example structured log:

```json
{
  "run_id": "run_123",
  "phase": "FilteringMapper",
  "partition": 4,
  "object_id": "Nikon",
  "LB": 3.41,
  "UB": 5.82,
  "tau": 4.90,
  "decision": "survive",
  "reason": "UB >= tau"
}
````

## 23. Partition and skew analysis

Distributed algorithms often fail because of skew.

The GUI should show:

| Skew metric                   | Meaning                     |                |
| ----------------------------- | --------------------------- | -------------- |
| Objects per partition         | load balance                |                |
| Instances per partition       | true compute load           |                |
| Candidates per partition      | reducer burden              |                |
| AES record size per partition | communication skew          |                |
| Longest mapper time           | straggler                   |                |
| Longest reducer time          | bottleneck                  |                |
| Max/median load ratio         | ni coefficient              | load imbalance |
| Hot partitions                | partitions causing slowdown |                |

A visual panel should show:

```text
Partition 7 is a straggler
- 2.3× more instances than median
- 3.1× more partially dominated MBRs
- 2.8× longer reducer time
Suggested action:
- increase partition count
- use spatial partitioning
- use load-aware partitioner
```

This would be especially important if future work adds adaptive load balancing, which the paper itself mentions as a future direction. fileciteturn4file0

## 24. Memory and communication inspector

Because the paper is about reducing unnecessary computation and emissions, the GUI should carefully inspect memory and communication.

Communication panel:

| Metric               |   Baseline | Proposed | Reduction |
| -------------------- | ---------: | -------: | --------: |
| Map output records   | 10,000,000 |  800,000 |       92% |
| Map output bytes     |     2.4 GB |   410 MB |       83% |
| Shuffle bytes        |     1.8 GB |   350 MB |       80% |
| Reduce input records | 10,000,000 |  800,000 |       92% |
| AER                  |          — |       8% |         — |

Memory panel:

| Metric                      |  Value |
| --------------------------- | -----: |
| Peak mapper heap            | 2.1 GB |
| Peak reducer heap           | 3.4 GB |
| GC time                     |  1.3 s |
| Spill count                 |     12 |
| Largest AES record          |  92 KB |
| Average competitor set size |     46 |

This would help verify whether AES reduces record count but accidentally creates huge individual records.

## 25. Report generator

The GUI should produce two kinds of reports.

### 25.1 Academic report

It should export:

| Output                     | Format          |
| -------------------------- | --------------- |
| Dataset description        | Markdown, LaTeX |
| Experiment setup           | Markdown, LaTeX |
| Hardware/software table    | LaTeX           |
| Baseline vs proposed table | LaTeX           |
| Ablation table             | LaTeX           |
| Plots                      | PNG, SVG, PDF   |
| Statistical summary        | CSV, LaTeX      |
| Reproducibility appendix   | Markdown        |
| Artifact checklist         | Markdown        |

Example generated academic paragraph:

```text
We evaluated Baseline, AES-only, DSCP-only, and AES+DSCP over 20 fixed-seed query points. For each run, we recorded filtering time, refinement time, wall-clock time, communication cost, emitted record count, and correctness against the baseline exact output. All algorithms were executed under identical hardware, JVM, Hadoop, partition, and reducer configurations.
```

### 25.2 Industry report

It should export:

| Output                  | Purpose               |
| ----------------------- | --------------------- |
| Executive summary       | Fast understanding    |
| SLA table               | latency/cost          |
| Regression summary      | release safety        |
| Cost estimate           | CPU, memory, network  |
| Failure analysis        | operational debugging |
| Capacity recommendation | cluster sizing        |
| Data-quality summary    | input trust           |

## 26. Reviewer mode

A special mode should be built for peer reviewers.

Reviewer mode should hide development clutter and show:

| Reviewer question                    | GUI answer                   |
| ------------------------------------ | ---------------------------- |
| Can I reproduce Table II?            | one-click run                |
| Can I reproduce Figure 4?            | one-click plot               |
| Are datasets included?               | manifest shown               |
| Are seeds included?                  | yes/no                       |
| Are algorithms documented?           | algorithm cards              |
| Are results exact?                   | validation panel             |
| Are ablations available?             | AES-only, DSCP-only          |
| Can I inspect raw intermediate data? | mapper/shuffle/reducer views |
| Can I export all results?            | artifact bundle              |

This would make the project much stronger for publication, demonstration, and artifact evaluation.

## 27. Security and governance for industry use

If the GUI is used inside a company or research lab, it needs governance.

Features:

| Feature                     | Purpose                                       |
| --------------------------- | --------------------------------------------- |
| User accounts               | researcher, reviewer, admin                   |
| Role-based permissions      | who can run, delete, export                   |
| Dataset access controls     | sensitive data protection                     |
| Audit logs                  | who ran what                                  |
| Immutable benchmark records | prevent result tampering                      |
| Signed artifacts            | trust exported results                        |
| SSO integration             | enterprise deployment                         |
| Secrets management          | cluster credentials                           |
| Data redaction              | protect raw records                           |
| Retention policies          | delete old intermediates                      |
| Approval workflow           | expensive cluster benchmark requires approval |

Academic prototypes often ignore this, but industry systems cannot.

## 28. Backend architecture

A practical architecture would look like this:

```text
Frontend
  React / Next.js / Vue
  Charts, map viewer, table viewer, graph viewer

API server
  FastAPI / Spring Boot
  Authentication
  Benchmark orchestration
  Dataset and run metadata

Benchmark runner
  Local runner
  Spark runner
  Docker runner
  Slurm/Kubernetes runner

DataSimulator service
  Synthetic generator
  Spatial generator
  Query generator
  Probability normalizer
  Dataset validator

Storage
  PostgreSQL for metadata
  filesystem for artifacts
  local path for large datasets
  Prometheus/OpenTelemetry backend for metrics

Algorithm plugins
  Baseline PTD
  AES-only
  DSCP-only
  AES+DSCP
  Brute-force oracle
```

The GUI should not hard-code only this one paper’s algorithm. It should support plugin algorithms so future PTD, skyline, probabilistic skyline, top-k, Spark, streaming, and approximate methods can be added.

## 29. Suggested database schema

Core tables:

```text
projects
datasets
dataset_versions
simulator_configs
query_sets
algorithms
algorithm_versions
benchmark_suites
benchmark_runs
run_metrics
run_counters
run_artifacts
validation_results
raw_inspection_samples
system_telemetry
comparison_reports
users
audit_logs
```

Important fields for `benchmark_runs`:

```text
run_id
project_id
dataset_version_id
query_set_id
algorithm_version_id
config_hash
seed
status
start_time
end_time
runtime_engine
hardware_profile_id
validation_status
artifact_path
```

Important fields for `run_metrics`:

```text
run_id
metric_name
metric_value
metric_unit
phase
timestamp
```

Important fields for PTD counters:

```text
run_id
partition_id
mapper_id
reducer_id
objects_before_dscp
objects_after_dscp
instances_before_dscp
instances_after_dscp
tau
baseline_emissions
aes_emissions
ddr_tests
mbr_full_count
mbr_partial_count
candidate_count
```

## 30. Minimum viable GUI vs full research GUI

A good MVP should not try to build everything at once.

### MVP

| Module             | Must have                                            |
| ------------------ | ---------------------------------------------------- |
| Dataset simulator  | synthetic smartphone + generic uncertain data        |
| Data inspector     | object-instance table, probability validation        |
| Algorithm runner   | baseline, AES-only, DSCP-only, AES+DSCP              |
| Benchmark designer | fixed parameter runs + repetitions                   |
| Metrics            | wall-clock, communication cost, AER, candidate count |
| Validation         | baseline agreement + small brute-force oracle        |
| Results            | tables and simple plots                              |
| Export             | CSV, Markdown, plots                                 |

### Strong academic version

Add:

| Module                 | Addition                        |
| ---------------------- | ------------------------------- |
| Road/OSM simulator     | map-based spatial uncertainty   |
| DDR/MBR visualization  | interactive dominance inspector |
| Statistical analysis   | confidence intervals, variance  |
| Reproducibility bundle | artifact export                 |
| Reviewer mode          | paper reproduction workflow     |
| Ablation report        | automatic AES/DSCP contribution |

### Industry-grade version

Add:

| Module                | Addition                  |
| --------------------- | ------------------------- |
| CI/CD integration     | regression gates          |
| Observability         | traces, logs, metrics     |
| Multi-user auth       | roles and audit           |
| Cluster orchestration | Kubernetes/Hadoop/Spark   |
| Cost dashboard        | CPU, memory, network cost |
| Data governance       | access control, retention |
| Alerts                | performance regressions   |

## 31. Ideal screen layout

A clean layout could be:

```text
Left sidebar:
  Dashboard
  Data Simulator
  Datasets
  Query Sets
  Algorithms
  Benchmark Suites
  Runs
  Validation
  Raw Inspector
  Results
  Reports
  Artifacts
  Settings

Main workspace:
  Current selected tool

Right inspector drawer:
  Selected object/run/metric details
```

For example, in **Raw Inspector**, the main view shows the table or visualization, while the right drawer shows selected-object details: LB, UB, τ, partition, DSCP decision, AES competitor set, final rank.

## 32. Example end-to-end workflow

A researcher should be able to do this:

1. Open **Data Simulator**.
2. Select **Synthetic Smartphone**.
3. Set:

   * 15 market regions
   * 50 brands per region
   * 8–20 models per brand
   * price and battery attributes
   * probabilities normalized per brand
   * 5% outliers
   * seed = 42
4. Generate dataset.
5. Open **Data Inspector** and verify probabilities.
6. Generate 20 query points.
7. Open **Benchmark Designer**.
8. Select algorithms:

   * baseline
   * AES-only
   * DSCP-only
   * AES+DSCP
9. Set repetitions = 20.
10. Run benchmark.
11. Wate/reducer metrics.
12. Open **Validation Center** and confirm exact top-k agreement.
13. Open **Results** and view:

* baseline vs proposed wall-clock time
* AES/DSCP ablation
* communication metrics
* pruning ratio

14. Open **Raw Inspector** and inspect one pruned object.
15. Export:

* LaTeX table
* SVG figure
* raw CSV
* reproducibility bundle.

This workflow directly supports the kind of results reported in the paper while making them far more transparent. fileciteturn4file0

# The most important GUI features, ranked

If you only have time to build the most valuable parts, prioritize these:

| Priority | Feature                       | Why                                           |
| -------: | ----------------------------- | --------------------------------------------- |
|        1 | Dataset simulator frontend    | Without controlled data, benchmarking is weak |
|        2 | Benchmark suite builder       | Needed for systematic experiments             |
|        3 | Exact validation center       | Prevents speed-only false claims              |
|        4 | Raw object/instance inspector | Makes uncertainty visible                     |
|        5 | DSCP/AES counters             | Proves why the method is faster               |
|        6 | Ablation runner               | Essential for academic contribution           |
|        7 | Reproducibility bundle        | Essential for publication/artifact review     |
|        8 | Pipeline trace viewer         | Essential for debugging                       |
|        9 | Partition/skew dashboard      | Essential for distributed systems             |
|       10 | Report generator              | Saves time and improves paper quality         |

# Final recommendation

The GUI should be positioned as a **benchmarking and validation laboratory for uncertain-database query algorithms**. For academic research, its strongest value is reproducibility, ablation, correctness validation, and paper-ready reporting. For industry research, its strongest value is observability, regression testing, resourc

-cost analysis, data-quality validation, and operational debugging.

The key design principle is this:

**Every number in a result table should be clickable.**

If the GUI shows “34.2% faster,” the user should be able to click it and see the exact runs, seeds, datasets, query points, algorithm configs, phase timings, counters, raw emissions, validation result, logs, and exported artifacts behind that number. That is the difference between a simple dashboard and a serious academic/industry research GUI.

[1]: https://mlflow.org/docs/latest/ml/tracking/?utm_source=chatgpt.com "ML Experiment Tracking | MLflow AI Platform"
[2]: https://github.com/sosy-lab/benchexec?utm_source=chatgpt.com "GitHub - sosy-lab/benchexec"
[3]: https://opentelemetry.io/docs/?utm_source=chatgpt.com "Documentation"
[4]: https://docs.greatexpectations.io/docs/0.18/reference/learn/terms/data_docs?utm_source=chatgpt.com "Data Docs | Great Expectations"
[5]: https://hadoop.apache.org/docs/r3.4.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html?utm_source=chatgpt.com "Apache Hadoop 3.4.3 – MapReduce Tutorial"
[6]: https://www.acm.org/publications/policies/artifact-review-badging?utm_source=chatgpt.com "Artifact Review and Badging – Version 1.0 (not current)"
[7]: https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html?utm_source=chatgpt.com "Apache Hadoop 3.3.5 – MapReduce Tutorial"
[8]: https://sigsim.acm.org/conf/pads/2024/blog/artifact-evaluation/?utm_source=chatgpt.com "Reproducibility and Artifact Evaluation"
[9]: https://www.oracle.com/technical-resources/articles/java/architect-benchmarking.html?utm_source=chatgpt.com "Avoiding Benchmarking Pitfalls on the JVM"


THERE SHOUDLD BE A WAY TO SAVE A RUN AND CAN BE USED TO BECH COMAPRITON AMONG SETUPS.