# E2E Benchmark

Pipeline:

```text
PythonSimulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka -> Apache Spark Structured Streaming bounded reader -> SparkTopKEngine -> TopKResult
```

Config:

- objects: 40
- queries: 1
- dimensions: 2
- k: 5
- missingRate: 0.35
- publisherRatePerSecond: 500
- mqttQos: 1
- sparkPartitions: 4
- synopsisBins: 8
- algorithm: aes-dscp
- expectedMessages: 310
- kafkaMessages: 310
- topKResults: 1
- dataset: csv
- datasetPath: /home/user/Projects/shafin-vai-thesis/datasets-curated/bangladesh-road-smoke.csv
- topicMappings: thesis/raw=thesis.raw.incomplete
- savedRunId: stream-bangladesh-road-smoke-qos1-20260527T110800Z

Timing:

- mqttPublishMs: 1371
- mqttToKafkaReadyMs: 2523
- sparkDrainMs: 13656
- totalE2EMs: 16181
- publishRateMessagesPerSecond: 226.11
- endToEndRateMessagesPerSecond: 19.16

Artifacts:

- Spark log: `reports/e2e/stream-bangladesh-road-smoke-qos1-20260527T110800Z/spark.log`
- Spark submit log: `reports/e2e/stream-bangladesh-road-smoke-qos1-20260527T110800Z/spark-submit.log`
- CSV: `reports/e2e/stream-bangladesh-road-smoke-qos1-20260527T110800Z/summary.csv`
