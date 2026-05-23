package com.thesis.topk.flink;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.dataset.DatasetProviders;
import com.thesis.topk.dataset.DatasetProvider;
import com.thesis.topk.dataset.DatasetRouting;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.io.RawEventJsonSerde;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class ProbabilisticTopKJob {
  private ProbabilisticTopKJob() {
  }

  public static void main(String[] args) throws Exception {
    Args parsed = Args.parse(args);
    SimulationConfig config = parsed.simulationConfig();
    long windowMs = parsed.longValue("windowMs", 30_000L);
    long outOfOrderMs = parsed.longValue("outOfOrderMs", 1_000L);
    String source = parsed.stringValue("source", "simulator");
    int parallelism = parsed.intValue("parallelism", 1);
    int synopsisBins = parsed.intValue("synopsisBins", 8);

    SimulationData simulation = DatasetProviders.generate(parsed);
    DdImputationSynopsis synopsis = DdImputationSynopsis.train(simulation.events(), synopsisBins);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    env.getConfig().setAutoWatermarkInterval(100L);
    System.out.printf(
        "pipelineConfig source=%s parallelism=%d windowMs=%d synopsisRules=%d synopsisBins=%d "
            + "synopsisAvgCandidateCount=%.3f%n",
        source,
        parallelism,
        windowMs,
        synopsis.ruleCount(),
        synopsis.bins(),
        synopsis.averageEstimatedCandidateCount());

    DataStream<RawEvent> raw = rawEvents(env, parsed, simulation, source)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<RawEvent>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderMs))
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<RawEvent>) (event, ignored) -> event.eventTime()));

    raw
        .map(new FlinkImputationFunction(synopsis))
        .keyBy(ImputedRecord::queryId)
        .process(new TopKProcessFunction(simulation.queryPoints(), config.k(), windowMs))
        .print();

    env.execute("paper-informed-probabilistic-topk");
    System.exit(0);
  }

  private static DataStream<RawEvent> rawEvents(StreamExecutionEnvironment env, Args parsed,
      SimulationData simulation, String source) {
    if ("kafka".equalsIgnoreCase(source)) {
      String bootstrap = parsed.stringValue("kafkaBootstrap", "localhost:9092");
      DatasetProvider provider = DatasetProviders.byName(parsed.stringValue("dataset", "synthetic"));
      List<String> topics = "all".equalsIgnoreCase(provider.name())
          ? DatasetProviders.allRawDatasets().stream().map(DatasetProvider::kafkaTopic).toList()
          : List.of(DatasetRouting.kafkaTopic(provider, parsed));
      String groupId = parsed.stringValue("kafkaGroupId", "probabilistic-topk-flink");
      boolean bounded = parsed.booleanValue("kafkaBounded", false);
      var builder = KafkaSource.<String>builder()
          .setBootstrapServers(bootstrap)
          .setTopics(topics)
          .setGroupId(groupId)
          .setStartingOffsets(OffsetsInitializer.earliest())
          .setValueOnlyDeserializer(new SimpleStringSchema());
      if (bounded) {
        builder.setBounded(OffsetsInitializer.latest());
      }
      KafkaSource<String> kafkaSource = builder.build();
      return env
          .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-raw-incomplete")
          .map(RawEventJsonSerde::fromJson);
    }
    if (!"simulator".equalsIgnoreCase(source)) {
      throw new IllegalArgumentException("Unsupported source: " + source);
    }
    return env.fromCollection(simulation.events());
  }
}
