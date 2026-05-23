package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.dataset.DatasetProviders;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.time.Duration;
import java.time.Instant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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
    String sparkMaster = parsed.stringValue("sparkMaster", "local[*]");

    SimulationData data = DatasetProviders.byName(dataset).generate(config, parsed);
    DdImputationSynopsis synopsis = DdImputationSynopsis.train(data.events(), synopsisBins);

    SparkConf conf = new SparkConf()
        .setAppName("probabilistic-topk-spark")
        .setMaster(sparkMaster)
        .set("spark.ui.enabled", parsed.stringValue("sparkUi", "false"))
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    Instant start = Instant.now();
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      sc.setLogLevel(parsed.stringValue("sparkLogLevel", "WARN"));
      SparkTopKEngine.SparkRunResult result = SparkTopKEngine.rank(
          sc,
          data.events(),
          data.queryPoints(),
          synopsis,
          k,
          partitions);
      printReport(dataset, k, partitions, synopsis, result, Duration.between(start, Instant.now()));
    }
  }

  private static void printReport(
      String dataset,
      int k,
      int partitions,
      DdImputationSynopsis synopsis,
      SparkTopKEngine.SparkRunResult result,
      Duration elapsed) {
    System.out.printf("engine=apache-spark dataset=%s k=%d partitions=%d elapsedMs=%d%n",
        dataset, k, partitions, elapsed.toMillis());
    System.out.printf("rawEvents=%d probabilisticInstances=%d synopsisRules=%d synopsisBins=%d%n",
        result.rawEventCount(),
        result.probabilisticInstanceCount(),
        synopsis.ruleCount(),
        synopsis.bins());

    for (SparkTopKEngine.QueryRanking ranking : result.rankings()) {
      System.out.printf(
          "query=%s objects=%d refined=%d pruned=%d pruneRatio=%.4f tau=%.6f compactShuffleRecords=%d%n",
          ranking.queryId(),
          ranking.objectCount(),
          ranking.refinedCount(),
          ranking.prunedCount(),
          ranking.pruneRatio(),
          ranking.pruningThreshold(),
          ranking.compactShuffleRecords());
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
