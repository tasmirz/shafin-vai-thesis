# Spark Upgrade Summary

The project has been upgraded so the required execution path uses Apache Spark.

## Changed

- Added `src/main/java/com/thesis/topk/spark/SparkTopKEngine.java`.
- Added `src/main/java/com/thesis/topk/spark/ProbabilisticTopKSparkJob.java`.
- Added Maven dependencies for Apache Spark Core and Spark SQL.
- Renamed the Maven artifact to `probabilistic-topk-spark`.
- Added `just spark` for local Spark execution.
- Added `Dockerfile.spark` for Spark submit execution.
- Updated README and validation notes to describe the Spark-first architecture.
- Added `reports/implementation/spark-upgrade-report.md`.

## Spark command

```bash
just spark
```

Direct Maven command:

```bash
mvn -q -DskipTests compile exec:java \
  -Dexec.mainClass=com.thesis.topk.spark.ProbabilisticTopKSparkJob \
  -Dexec.args="--dataset=synthetic --objects=200 --queries=2 --dimensions=4 --k=5 --partitions=4 --sparkMaster=local[*]"
```

## Notes

The existing Flink/Kafka files remain for comparison, but the upgraded execution entry point is Spark.
