# Paper Alignment Validation Report

This report describes the Spark-upgraded project state.

## Source Paper Alignment

The supplied 2025 ICCIT work proposes a distributed PTD framework with:

- Dynamic Smart Candidate Pruning (DSCP)
- Aggregated Emission Strategy (AES)
- distributed filtering/refinement phases
- Hadoop/MapReduce execution in the original paper
- future extension potential on Apache Spark

This repository implements that upgrade direction by using Apache Spark as the distributed execution layer.

## Implemented Spark Mapping

| Paper idea | Spark project mapping |
|---|---|
| uncertain objects and probabilistic instances | raw events repaired into `ProbabilisticInstance` objects |
| DDR-style query-relative dominance | `DominanceScorer` and query-aware ranking |
| LB/UB candidate bounds | Spark object-group candidate envelopes |
| DSCP pruning | kth lower-bound threshold, pruning objects with weak upper bounds |
| AES compact emissions | object-level grouped Spark shuffle instead of instance-competitor emissions |
| exact refinement | surviving candidate groups are rescored exactly |

## Runtime Validation Surface

The full runtime path is now:

```text
PythonSimulator -> EMQX MQTT -> Kafka -> Apache Spark bounded Kafka reader -> SparkTopKEngine -> TopKResult
```

Validation commands:

```bash
just spark
just image
just setup
just spark-submit
just e2e
just validate
just test-all
```

## Deployment Validation Surface

Updated files:

- `Dockerfile`
- `Dockerfile.spark`
- `docker-compose.e2e.yml`
- `k8s/pipeline.yaml`
- `scripts/setup-services.sh`
- `scripts/e2e-benchmark.sh`
- `scripts/validate-e2e.py`
- `scripts/monitor.py`
- `Justfile`

## Notes

The older Flink package remains available only as reference code. The deployment and reproducibility path is Spark-first.
