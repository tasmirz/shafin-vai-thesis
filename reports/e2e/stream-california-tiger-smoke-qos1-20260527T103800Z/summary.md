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
- datasetPath: /home/user/Projects/shafin-vai-thesis/datasets-curated/california-tiger-road-smoke.csv
- topicMappings: thesis/raw=thesis.raw.incomplete
- savedRunId: stream-california-tiger-smoke-qos1-20260527T103800Z

Timing:

- mqttPublishMs: 1362
- mqttToKafkaReadyMs: 2488
- sparkDrainMs: 11619
- totalE2EMs: 14110
- publishRateMessagesPerSecond: 227.61
- endToEndRateMessagesPerSecond: 21.97

Artifacts:

- Spark log: `reports/e2e/stream-california-tiger-smoke-qos1-20260527T103800Z/spark.log`
- Spark submit log: `reports/e2e/stream-california-tiger-smoke-qos1-20260527T103800Z/spark-submit.log`
- CSV: `reports/e2e/stream-california-tiger-smoke-qos1-20260527T103800Z/summary.csv`
