# E2E Benchmark

Pipeline:

```text
PythonSimulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka -> Apache Spark bounded Kafka reader -> SparkTopKEngine -> TopKResult
```

Config:

- objects: 200
- queries: 2
- dimensions: 4
- k: 10
- missingRate: 0.35
- publisherRatePerSecond: 200
- mqttQos: 0
- sparkPartitions: 4
- synopsisBins: 8
- expectedMessages: 400
- kafkaMessages: 400
- topKResults: 2
- dataset: synthetic
- datasetPath: 
- topicMappings: thesis/raw=thesis.raw.incomplete

Timing:

- mqttPublishMs: 2586
- mqttToKafkaReadyMs: 3894
- sparkDrainMs: 17077
- totalE2EMs: 20973
- publishRateMessagesPerSecond: 154.68
- endToEndRateMessagesPerSecond: 19.07

Artifacts:

- Spark log: `reports/e2e/spark.log`
- Spark submit log: `reports/e2e/spark-submit.log`
- CSV: `reports/e2e/summary.csv`
