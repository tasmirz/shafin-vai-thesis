package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.variant.PtdAlgorithm;
import com.thesis.topk.algorithm.variant.PtdAlgorithmRegistry;
import com.thesis.topk.dataset.DatasetProviders;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import com.thesis.topk.spark.SparkTopKEngine.QueryRanking;
import com.thesis.topk.spark.SparkTopKEngine.SparkRunResult;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * CLI entry point for the HGTP (improved) probabilistic top-k dominance engine.
 *
 * <p>Usage is identical to {@link ProbabilisticTopKSparkJob} but defaults to
 * {@code --algorithm=improved-aes-dscp} and reports {@code engine=improved-apache-spark}.
 * All dataset loading and output formatting follows the same conventions.</p>
 */
public final class ImprovedProbabilisticTopKSparkJob {
  private ImprovedProbabilisticTopKSparkJob() {}

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
    int traceLimit = parsed.intValue("traceLimit", 25);
    String resultsFile = parsed.stringValue("resultsFile", null);
    PtdAlgorithm algorithm = PtdAlgorithmRegistry.require(
        parsed.stringValue("algorithm", PtdAlgorithmRegistry.IMPROVED_DEFAULT_ID));

    SparkConf conf = new SparkConf()
        .setAppName("improved-probabilistic-topk-spark")
        .setMaster(sparkMaster)
        .set("spark.ui.enabled", parsed.stringValue("sparkUi", "false"))
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    Instant start = Instant.now();
    try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
      sc.setLogLevel(parsed.stringValue("sparkLogLevel", "WARN"));
      SimulationData data = DatasetProviders.byName(dataset).generate(config, parsed);
      DdImputationSynopsis synopsis = DdImputationSynopsis.train(data.events(), synopsisBins);
      String boundMode = boundMode(data.events());

      SparkRunResult result = ImprovedSparkTopKEngine.rank(
          sc, data.events(), data.queryPoints(), synopsis,
          k, partitions, algorithm, validateExact, traceLimit);

      printReport(dataset, source, k, partitions, algorithm, boundMode,
          synopsis, result, Duration.between(start, Instant.now()));
      if (resultsFile != null) {
        saveResultsToFile(resultsFile, result);
      }
    }
  }

  private static String boundMode(List<RawEvent> events) {
    List<RawEvent> upserts = events.stream()
        .filter(event -> event.opType() == OpType.UPSERT).toList();
    return !upserts.isEmpty() && upserts.stream().allMatch(RawEvent::hasMbr)
        ? "hgtp-artree-selected-level"
        : "hgtp-conservative-remote-mass";
  }

  private static void printReport(
      String dataset, String source, int k, int partitions,
      PtdAlgorithm algorithm, String boundMode,
      DdImputationSynopsis synopsis, SparkRunResult result, Duration elapsed) {
    long validationMs = Duration.ofNanos(result.validationNanos()).toMillis();
    long algorithmElapsedMs = result.rankings().stream()
        .mapToLong(r -> r.filteringNanos() + r.emissionNanos() + r.refinementNanos())
        .sum() / 1_000_000L;
    long setupMs = Math.max(0L, elapsed.toMillis() - validationMs - algorithmElapsedMs);
    System.out.printf(
        "engine=improved-apache-spark source=%s dataset=%s k=%d partitions=%d elapsedMs=%d "
            + "algorithmElapsedMs=%d setupMs=%d validationMs=%d algorithm=%s dscp=%s aes=%s "
            + "boundMode=%s emissionScope=server-partition%n",
        source, dataset, k, partitions, elapsed.toMillis(), algorithmElapsedMs, setupMs,
        validationMs, algorithm.id(), algorithm.dscpEnabled(), algorithm.aesEnabled(), boundMode);
    System.out.printf("rawEvents=%d probabilisticInstances=%d synopsisRules=%d synopsisBins=%d%n",
        result.rawEventCount(), result.probabilisticInstanceCount(),
        synopsis.ruleCount(), synopsis.bins());

    for (QueryRanking ranking : result.rankings()) {
      System.out.printf(
          "TopKResult{engine=improved-apache-spark, algorithm=%s, queryId=%s, objects=%d, "
              + "refined=%d, pruned=%d, pruneRatio=%.4f, tau=%.6f, emittedRecords=%d, "
              + "baselineEmissions=%d, aesEmissions=%d, AER=%.6f, falsePrunes=%d, "
              + "indexedMbrPath=%s, partialMbrRefs=%d, "
              + "filterMs=%d, emissionMs=%d, refineMs=%d, shuffleRecords=%d, shuffleBytes=%d, "
              + "tasks=%d, executorRunMs=%d, gcMs=%d, stragglerRatio=%.4f, "
              + "validationPerformed=%s, exactAgreement=%s}%n",
          ranking.algorithmId(), ranking.queryId(),
          ranking.objectCount(), ranking.refinedCount(), ranking.prunedCount(),
          ranking.pruneRatio(), ranking.pruningThreshold(), ranking.emittedRecords(),
          ranking.baselineEmissions(), ranking.aesEmissions(),
          ranking.aggregatedEmissionRate(), ranking.falsePruneCount(),
          ranking.indexedMbrPath(), ranking.partialMbrReferences(),
          Duration.ofNanos(ranking.filteringNanos()).toMillis(),
          Duration.ofNanos(ranking.emissionNanos()).toMillis(),
          Duration.ofNanos(ranking.refinementNanos()).toMillis(),
          ranking.observedMetrics().shuffleWriteRecords(),
          ranking.observedMetrics().shuffleWriteBytes(),
          ranking.observedMetrics().taskCount(),
          ranking.observedMetrics().executorRunTimeMs(),
          ranking.observedMetrics().jvmGcTimeMs(),
          ranking.observedMetrics().stragglerRatio(),
          ranking.validationPerformed(), ranking.exactAgreement());
      System.out.printf(
          "query=%s algorithm=%s objects=%d refined=%d pruned=%d pruneRatio=%.4f tau=%.6f "
              + "emittedRecords=%d baselineEmissions=%d aesEmissions=%d AER=%.6f "
              + "falsePrunes=%d indexedMbrPath=%s partialMbrRefs=%d "
              + "filterMs=%d emissionMs=%d refineMs=%d shuffleRecords=%d "
              + "shuffleBytes=%d tasks=%d executorRunMs=%d gcMs=%d stragglerRatio=%.4f "
              + "validationPerformed=%s exactAgreement=%s%n",
          ranking.queryId(), ranking.algorithmId(),
          ranking.objectCount(), ranking.refinedCount(), ranking.prunedCount(),
          ranking.pruneRatio(), ranking.pruningThreshold(), ranking.emittedRecords(),
          ranking.baselineEmissions(), ranking.aesEmissions(),
          ranking.aggregatedEmissionRate(), ranking.falsePruneCount(),
          ranking.indexedMbrPath(), ranking.partialMbrReferences(),
          Duration.ofNanos(ranking.filteringNanos()).toMillis(),
          Duration.ofNanos(ranking.emissionNanos()).toMillis(),
          Duration.ofNanos(ranking.refinementNanos()).toMillis(),
          ranking.observedMetrics().shuffleWriteRecords(),
          ranking.observedMetrics().shuffleWriteBytes(),
          ranking.observedMetrics().taskCount(),
          ranking.observedMetrics().executorRunTimeMs(),
          ranking.observedMetrics().jvmGcTimeMs(),
          ranking.observedMetrics().stragglerRatio(),
          ranking.validationPerformed(), ranking.exactAgreement());
      for (CandidateScore score : ranking.topK()) {
        System.out.printf("  rank object=%s score=%.6f lb=%.6f ub=%.6f instances=%d%n",
            score.objectId(), score.exactScore(), score.lowerBound(),
            score.upperBound(), score.instanceCount());
      }
      for (SparkTopKEngine.ObjectTrace trace : ranking.objectTraces()) {
        System.out.printf(
            "ObjectTrace{queryId=%s, objectId=%s, partition=%d, lb=%.6f, ub=%.6f, "
                + "tau=%.6f, decision=%s, partialMbrRefs=%d, baselineEmissions=%d, "
                + "aesEmissions=%d}%n",
            trace.queryId(), trace.objectId(), trace.partitionId(),
            trace.lowerBound(), trace.upperBound(), trace.tau(), trace.decision(),
            trace.partialMbrReferences(), trace.baselineEmissions(), trace.aesEmissions());
      }
    }
  }

  private static void saveResultsToFile(String filePath, SparkRunResult result) {
    try (java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter(filePath))) {
      writer.println("query_id,rank,object_id,score,lower_bound,upper_bound,instances");
      for (QueryRanking ranking : result.rankings()) {
        int rank = 1;
        for (CandidateScore score : ranking.topK()) {
          writer.printf("%s,%d,%s,%.6f,%.6f,%.6f,%d%n",
              ranking.queryId(), rank++, score.objectId(),
              score.exactScore(), score.lowerBound(), score.upperBound(), score.instanceCount());
        }
      }
      System.out.println("Saved Top-K results to: " + filePath);
    } catch (java.io.IOException e) {
      System.err.println("Failed to write results file: " + e.getMessage());
    }
  }
}
