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
- expectedMessages: 15
- kafkaMessages: 15
- topKResults: 15
- dataset: all
- datasetPath: 
- topicMappings: thesis/raw/intel=thesis.raw.intel,thesis/raw/pump=thesis.raw.pump,thesis/raw/gas=thesis.raw.gas

Timing:

- mqttPublishMs: 548
- mqttToKafkaReadyMs: 1678
- flinkDrainMs: 3884
- totalE2EMs: 5563
- publishRateMessagesPerSecond: 27.37
- endToEndRateMessagesPerSecond: 2.70

Artifacts:

- Flink log: `reports/e2e/flink.log`
- Flink submit log: `reports/e2e/flink-submit.log`
- CSV: `reports/e2e/summary.csv`
