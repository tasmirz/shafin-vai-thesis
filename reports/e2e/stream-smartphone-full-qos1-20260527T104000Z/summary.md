# E2E Benchmark

Pipeline:

```text
PythonSimulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka -> Apache Spark Structured Streaming bounded reader -> SparkTopKEngine -> TopKResult
```

Config:

- objects: 750
- queries: 20
- dimensions: 2
- k: 10
- missingRate: 0.35
- publisherRatePerSecond: 0
- mqttQos: 1
- sparkPartitions: 8
- synopsisBins: 8
- algorithm: aes-dscp
- expectedMessages: 207860
- kafkaMessages: 207860
- topKResults: 20
- dataset: csv
- datasetPath: /home/user/Projects/shafin-vai-thesis/datasets-curated/smartphone-paper.csv
- topicMappings: thesis/raw=thesis.raw.incomplete
- savedRunId: stream-smartphone-full-qos1-20260527T104000Z

Timing:

- mqttPublishMs: 1531756
- mqttToKafkaReadyMs: 1533338
- sparkDrainMs: 78981
- totalE2EMs: 1612321
- publishRateMessagesPerSecond: 135.70
- endToEndRateMessagesPerSecond: 128.92

Artifacts:

- Spark log: `reports/e2e/stream-smartphone-full-qos1-20260527T104000Z/spark.log`
- Spark submit log: `reports/e2e/stream-smartphone-full-qos1-20260527T104000Z/spark-submit.log`
- CSV: `reports/e2e/stream-smartphone-full-qos1-20260527T104000Z/summary.csv`
