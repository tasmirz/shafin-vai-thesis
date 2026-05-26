package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.variant.PtdAlgorithm;
import com.thesis.topk.algorithm.variant.PtdAlgorithmRegistry;
import com.thesis.topk.dataset.DatasetProvider;
import com.thesis.topk.dataset.DatasetProviders;
import com.thesis.topk.dataset.DatasetRouting;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.OpType;
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
    PtdAlgorithm algorithm = PtdAlgorithmRegistry.require(
        parsed.stringValue("algorithm", PtdAlgorithmRegistry.DEFAULT_ID));

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
      String boundMode = boundMode(data.events());
      SparkTopKEngine.SparkRunResult result = SparkTopKEngine.rank(
          sc,
          data.events(),
          data.queryPoints(),
          synopsis,
          k,
          partitions,
          algorithm,
          validateExact);
      printReport(dataset, source, k, partitions, algorithm, boundMode, synopsis, result,
          Duration.between(start, Instant.now()));
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

  private static String boundMode(List<RawEvent> events) {
    List<RawEvent> upserts = events.stream()
        .filter(event -> event.opType() == OpType.UPSERT)
        .toList();
    return !upserts.isEmpty() && upserts.stream().allMatch(RawEvent::hasMbr)
        ? "rai-lian-artree-selected-level-partial-reducer"
        : "conservative-remote-mass-no-mbr";
  }

  private static void printReport(
      String dataset,
      String source,
      int k,
      int partitions,
      PtdAlgorithm algorithm,
      String boundMode,
      DdImputationSynopsis synopsis,
      SparkTopKEngine.SparkRunResult result,
      Duration elapsed) {
    long validationMs = Duration.ofNanos(result.validationNanos()).toMillis();
    long algorithmElapsedMs = Math.max(0L, elapsed.toMillis() - validationMs);
    System.out.printf(
        "engine=apache-spark source=%s dataset=%s k=%d partitions=%d elapsedMs=%d "
            + "algorithmElapsedMs=%d validationMs=%d algorithm=%s dscp=%s aes=%s "
            + "boundMode=%s emissionScope=server-partition%n",
        source, dataset, k, partitions, elapsed.toMillis(), algorithmElapsedMs, validationMs,
        algorithm.id(), algorithm.dscpEnabled(), algorithm.aesEnabled(), boundMode);
    System.out.printf("rawEvents=%d probabilisticInstances=%d synopsisRules=%d synopsisBins=%d%n",
        result.rawEventCount(),
        result.probabilisticInstanceCount(),
        synopsis.ruleCount(),
        synopsis.bins());

    for (SparkTopKEngine.QueryRanking ranking : result.rankings()) {
      System.out.printf(
          "TopKResult{engine=apache-spark, algorithm=%s, queryId=%s, objects=%d, refined=%d, "
              + "pruned=%d, pruneRatio=%.4f, tau=%.6f, emittedRecords=%d, "
              + "baselineEmissions=%d, aesEmissions=%d, AER=%.6f, falsePrunes=%d, "
              + "indexedMbrPath=%s, partialMbrRefs=%d, "
              + "filterMs=%d, emissionMs=%d, refineMs=%d, shuffleRecords=%d, shuffleBytes=%d, "
              + "tasks=%d, executorRunMs=%d, gcMs=%d, stragglerRatio=%.4f, "
              + "validationPerformed=%s, exactAgreement=%s}%n",
          ranking.algorithmId(),
          ranking.queryId(),
          ranking.objectCount(),
          ranking.refinedCount(),
          ranking.prunedCount(),
          ranking.pruneRatio(),
          ranking.pruningThreshold(),
          ranking.emittedRecords(),
          ranking.baselineEmissions(),
          ranking.aesEmissions(),
          ranking.aggregatedEmissionRate(),
          ranking.falsePruneCount(),
          ranking.indexedMbrPath(),
          ranking.baselineEmissions(),
          Duration.ofNanos(ranking.filteringNanos()).toMillis(),
          Duration.ofNanos(ranking.emissionNanos()).toMillis(),
          Duration.ofNanos(ranking.refinementNanos()).toMillis(),
          ranking.observedMetrics().shuffleWriteRecords(),
          ranking.observedMetrics().shuffleWriteBytes(),
          ranking.observedMetrics().taskCount(),
          ranking.observedMetrics().executorRunTimeMs(),
          ranking.observedMetrics().jvmGcTimeMs(),
          ranking.observedMetrics().stragglerRatio(),
          ranking.validationPerformed(),
          ranking.exactAgreement());
      System.out.printf(
          "query=%s algorithm=%s objects=%d refined=%d pruned=%d pruneRatio=%.4f tau=%.6f "
              + "emittedRecords=%d baselineEmissions=%d aesEmissions=%d AER=%.6f "
              + "falsePrunes=%d indexedMbrPath=%s partialMbrRefs=%d "
              + "filterMs=%d emissionMs=%d refineMs=%d shuffleRecords=%d "
              + "shuffleBytes=%d tasks=%d executorRunMs=%d gcMs=%d stragglerRatio=%.4f "
              + "validationPerformed=%s exactAgreement=%s%n",
          ranking.queryId(),
          ranking.algorithmId(),
          ranking.objectCount(),
          ranking.refinedCount(),
          ranking.prunedCount(),
          ranking.pruneRatio(),
          ranking.pruningThreshold(),
          ranking.emittedRecords(),
          ranking.baselineEmissions(),
          ranking.aesEmissions(),
          ranking.aggregatedEmissionRate(),
          ranking.falsePruneCount(),
          ranking.indexedMbrPath(),
          ranking.baselineEmissions(),
          Duration.ofNanos(ranking.filteringNanos()).toMillis(),
          Duration.ofNanos(ranking.emissionNanos()).toMillis(),
          Duration.ofNanos(ranking.refinementNanos()).toMillis(),
          ranking.observedMetrics().shuffleWriteRecords(),
          ranking.observedMetrics().shuffleWriteBytes(),
          ranking.observedMetrics().taskCount(),
          ranking.observedMetrics().executorRunTimeMs(),
          ranking.observedMetrics().jvmGcTimeMs(),
          ranking.observedMetrics().stragglerRatio(),
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
