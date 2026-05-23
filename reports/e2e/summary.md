# E2E Benchmark

Pipeline:

```text
MqttIncompleteDataPublisher -> EMQX MQTT -> EMQX Kafka sink -> Kafka -> Flink bounded Kafka source -> TopKResult
```

Config:

- objects: 100
- queries: 2
- dimensions: 4
- k: 10
- missingRate: 0.35
- publisherRatePerSecond: 200
- mqttQos: 0
- expectedMessages: 200
- kafkaMessages: 200
- topKResults: 200
- dataset: synthetic
- datasetPath: 
- topicMappings: thesis/raw=thesis.raw.incomplete

Timing:

- mqttPublishMs: 1913
- mqttToKafkaReadyMs: 3505
- flinkDrainMs: 9294
- totalE2EMs: 12801
- publishRateMessagesPerSecond: 104.55
- endToEndRateMessagesPerSecond: 15.62

Artifacts:

- Flink log: `reports/e2e/flink.log`
- Flink submit log: `reports/e2e/flink-submit.log`
- CSV: `reports/e2e/summary.csv`
