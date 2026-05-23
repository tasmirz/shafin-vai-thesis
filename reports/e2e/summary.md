# E2E Benchmark

Pipeline:

```text
PythonSimulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka -> Flink bounded Kafka source -> TopKResult
```

Config:

- objects: 5
- queries: 1
- dimensions: 4
- k: 2
- missingRate: 0.2
- publisherRatePerSecond: 200
- mqttQos: 0
- flinkParallelism: 2
- synopsisBins: 8
- expectedMessages: 15
- kafkaMessages: 15
- topKResults: 15
- dataset: all
- datasetPath: 
- topicMappings: thesis/raw/intel=thesis.raw.intel,thesis/raw/pump=thesis.raw.pump,thesis/raw/gas=thesis.raw.gas

Timing:

- mqttPublishMs: 654
- mqttToKafkaReadyMs: 1884
- flinkDrainMs: 5455
- totalE2EMs: 7340
- publishRateMessagesPerSecond: 22.94
- endToEndRateMessagesPerSecond: 2.04

Artifacts:

- Flink log: `reports/e2e/flink.log`
- Flink submit log: `reports/e2e/flink-submit.log`
- CSV: `reports/e2e/summary.csv`
