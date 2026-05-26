package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.dataset.DatasetProvider;
import com.thesis.topk.dataset.DatasetProviders;
import com.thesis.topk.dataset.DatasetRouting;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/** Entry point for the Apache Spark upgrade of the probabilistic top-k dominance prototype. */
public final class ProbabilisticTopKSparkJob {
  private ProbabilisticTopKSparkJob() {
  }

  public static void main(String[] args) {
    Args parsed = Args.parse(args);
    SimulationConfig config = parsed.simulationConfig();
    int k = parsed.intValue("k", config.k());
    int partitions = parsed.intValue("partitions", parsed.intValue("parallelism", 4));
    int synopsisBins = parsed.intValue("synopsisBins", 8);
    String dataset = parsed.stringValue("dataset", "synthetic");
    String source = parsed.stringValue("source", "simulator");
    String sparkMaster = parsed.stringValue("sparkMaster", "local[*]");
    boolean validateExact = parsed.booleanValue("validateExact", false);

    SparkConf conf = new SparkConf()
        .setAppName("probabilistic-topk-spark")
        .setMaster(sparkMaster)
        .set("spark.ui.enabled", parsed.stringValue("sparkUi", "false"))
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    Instant start = Instant.now();
    try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
      sc.setLogLevel(parsed.stringValue("sparkLogLevel", "WARN"));
      SimulationData data = loadData(spark, parsed, config, dataset, source);
      DdImputationSynopsis synopsis = DdImputationSynopsis.train(data.events(), synopsisBins);
      SparkTopKEngine.SparkRunResult result = SparkTopKEngine.rank(
          sc,
          data.events(),
          data.queryPoints(),
          synopsis,
          k,
          partitions,
          validateExact);
      printReport(dataset, source, k, partitions, synopsis, result, Duration.between(start, Instant.now()));
    }
  }

  private static SimulationData loadData(
      SparkSession spark,
      Args parsed,
      SimulationConfig config,
      String dataset,
      String source) {
    SimulationData generated = DatasetProviders.byName(dataset).generate(config, parsed);
    if ("simulator".equalsIgnoreCase(source)) {
      return generated;
    }
    if (!"kafka".equalsIgnoreCase(source)) {
      throw new IllegalArgumentException("Unsupported Spark source: " + source);
    }

    List<String> topics = kafkaTopics(dataset, parsed);
    int expectedMessages = expectedKafkaMessages(parsed, config, topics.size());
    List<RawEvent> kafkaEvents = SparkKafkaStreamReader.readAvailable(
        spark, parsed, topics, expectedMessages);
    return new SimulationData(List.copyOf(kafkaEvents), generated.rules(), generated.queryPoints());
  }

  private static List<String> kafkaTopics(String dataset, Args parsed) {
    DatasetProvider provider = DatasetProviders.byName(dataset);
    if ("all".equalsIgnoreCase(provider.name())) {
      return DatasetProviders.allRawDatasets().stream().map(DatasetProvider::kafkaTopic).toList();
    }
    return List.of(DatasetRouting.kafkaTopic(provider, parsed));
  }

  private static int expectedKafkaMessages(Args parsed, SimulationConfig config, int topicCount) {
    if (parsed.has("expectedMessages")) {
      return parsed.intValue("expectedMessages", 0);
    }
    int maxEvents = parsed.intValue("maxEvents", 0);
    if (maxEvents > 0) {
      return maxEvents * Math.max(1, topicCount);
    }
    return Math.max(1, config.objects() * config.queries() * Math.max(1, topicCount));
  }

  private static void printReport(
      String dataset,
      String source,
      int k,
      int partitions,
      DdImputationSynopsis synopsis,
      SparkTopKEngine.SparkRunResult result,
      Duration elapsed) {
    long validationMs = Duration.ofNanos(result.validationNanos()).toMillis();
    long algorithmElapsedMs = Math.max(0L, elapsed.toMillis() - validationMs);
    System.out.printf(
        "engine=apache-spark source=%s dataset=%s k=%d partitions=%d elapsedMs=%d "
            + "algorithmElapsedMs=%d validationMs=%d%n",
        source, dataset, k, partitions, elapsed.toMillis(), algorithmElapsedMs, validationMs);
    System.out.printf("rawEvents=%d probabilisticInstances=%d synopsisRules=%d synopsisBins=%d%n",
        result.rawEventCount(),
        result.probabilisticInstanceCount(),
        synopsis.ruleCount(),
        synopsis.bins());

    for (SparkTopKEngine.QueryRanking ranking : result.rankings()) {
      System.out.printf(
          "TopKResult{engine=apache-spark, queryId=%s, objects=%d, refined=%d, pruned=%d, pruneRatio=%.4f, tau=%.6f, compactShuffleRecords=%d, validationPerformed=%s, exactAgreement=%s}%n",
          ranking.queryId(),
          ranking.objectCount(),
          ranking.refinedCount(),
          ranking.prunedCount(),
          ranking.pruneRatio(),
          ranking.pruningThreshold(),
          ranking.compactShuffleRecords(),
          ranking.validationPerformed(),
          ranking.exactAgreement());
      System.out.printf(
          "query=%s objects=%d refined=%d pruned=%d pruneRatio=%.4f tau=%.6f compactShuffleRecords=%d validationPerformed=%s exactAgreement=%s%n",
          ranking.queryId(),
          ranking.objectCount(),
          ranking.refinedCount(),
          ranking.prunedCount(),
          ranking.pruneRatio(),
          ranking.pruningThreshold(),
          ranking.compactShuffleRecords(),
          ranking.validationPerformed(),
          ranking.exactAgreement());
      for (CandidateScore score : ranking.topK()) {
        System.out.printf("  rank object=%s score=%.6f lb=%.6f ub=%.6f instances=%d%n",
            score.objectId(),
            score.exactScore(),
            score.lowerBound(),
            score.upperBound(),
            score.instanceCount());
      }
    }
  }
}
