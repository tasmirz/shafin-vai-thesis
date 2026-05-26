package com.thesis.topk.benchmark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.ImputationEngine;
import com.thesis.topk.algorithm.ProbabilisticTopK;
import com.thesis.topk.dataset.DatasetProviders;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class TopKBenchmark {
  private TopKBenchmark() {
  }

  public static void main(String[] args) {
    Args parsed = Args.parse(args);
    SimulationConfig config = parsed.simulationConfig();
    int candidateMultiplier = parsed.intValue("candidateMultiplier", 4);
    int partitions = parsed.intValue("partitions", 4);
    int synopsisBins = parsed.intValue("synopsisBins", 8);
    String dataset = parsed.stringValue("dataset", "synthetic");
    SimulationData data = DatasetProviders.byName(dataset).generate(config, parsed);
    DataSplit split = holdoutSplit(data.events());
    DdImputationSynopsis validationSynopsis = DdImputationSynopsis.train(split.training(), synopsisBins);
    DdImputationSynopsis.Evaluation imputation = validationSynopsis.evaluate(split.holdout());
    // Ranking must use the same complete training population as the Spark benchmark under test.
    DdImputationSynopsis synopsis = DdImputationSynopsis.train(data.events(), synopsisBins);
    List<ProbabilisticInstance> instances = new ArrayList<>();
    data.events().forEach(event -> instances.addAll(ImputationEngine.impute(event, synopsis)));
    int eventObjectCount = (int) data.events().stream().map(RawEvent::objectId).distinct().count();
    int dimensions = data.events().isEmpty() ? config.dimensions() : data.events().get(0).attributes().length;

    System.out.printf(
        "dataset provider=%s objects=%d events=%d instances=%d dimensions=%d queries=%d k=%d missingRate=%.3f "
            + "seed=%d partitions=%d executionEngine=java-local partitionModelNodes=%d "
            + "shuffleMetric=calculated-candidate-proxy%n",
        dataset,
        eventObjectCount,
        data.events().size(),
        instances.size(),
        dimensions,
        data.queryPoints().size(),
        config.k(),
        config.missingRate(),
        config.seed(),
        partitions,
        partitions);
    System.out.printf(
        "imputationSynopsis rules=%d bins=%d avgCandidateCount=%.3f trainingEvents=%d "
            + "holdoutEvents=%d evaluatedValues=%d holdoutMAE=%s%n",
        synopsis.ruleCount(),
        synopsis.bins(),
        synopsis.averageEstimatedCandidateCount(),
        split.training().size(),
        split.holdout().size(),
        imputation.evaluatedValues(),
        Double.isNaN(imputation.meanAbsoluteError())
            ? "n/a"
            : String.format("%.6f", imputation.meanAbsoluteError()));

    double totalExactMs = 0.0;
    double totalCertifiedMs = 0.0;
    double totalFastMs = 0.0;
    double totalCertifiedPruneRatio = 0.0;
    double totalFastPruneRatio = 0.0;
    double totalFastPrecision = 0.0;
    double totalPartitionedShuffleWrite = 0.0;
    double totalPartitionedCommunicationReduction = 0.0;
    double totalPartitionedPrecision = 0.0;
    int queryCount = 0;

    for (Map.Entry<String, QueryPoint> query : data.queryPoints().entrySet()) {
      List<ProbabilisticInstance> queryInstances = instances.stream()
          .filter(i -> i.queryId().equals(query.getKey()))
          .toList();
      int objectCount = (int) queryInstances.stream().map(ProbabilisticInstance::objectId).distinct().count();
      int candidateLimit = Math.min(objectCount, Math.max(config.k(), config.k() * candidateMultiplier));

      long exactStart = System.nanoTime();
      List<CandidateScore> exact = ProbabilisticTopK.exactTopK(queryInstances, query.getValue(), config.k());
      long exactNanos = System.nanoTime() - exactStart;

      long certifiedStart = System.nanoTime();
      ProbabilisticTopK.PrunedRanking certified =
          ProbabilisticTopK.prunedTopK(queryInstances, query.getValue(), config.k());
      long certifiedNanos = System.nanoTime() - certifiedStart;

      long fastStart = System.nanoTime();
      ProbabilisticTopK.PrunedRanking fast =
          ProbabilisticTopK.candidatePrunedTopK(queryInstances, query.getValue(), config.k(), candidateLimit);
      long fastNanos = System.nanoTime() - fastStart;

      PartitionedRanking partitioned = partitionedCandidateRanking(
          queryInstances, query.getValue(), config.k(), partitions, candidateMultiplier);

      List<String> exactIds = exact.stream().map(CandidateScore::objectId).toList();
      List<String> certifiedIds = certified.topK().stream().map(CandidateScore::objectId).toList();
      List<String> fastIds = fast.topK().stream().map(CandidateScore::objectId).toList();
      List<String> partitionedIds = partitioned.topK().stream().map(CandidateScore::objectId).toList();
      boolean certifiedAgrees = exactIds.equals(certifiedIds);
      double fastPrecisionAtK = precisionAtK(exactIds, fastIds);
      double partitionedPrecisionAtK = precisionAtK(exactIds, partitionedIds);
      double certifiedMs = certifiedNanos / 1_000_000.0;
      double fastMs = fastNanos / 1_000_000.0;
      double exactMs = exactNanos / 1_000_000.0;
      totalExactMs += exactMs;
      totalCertifiedMs += certifiedMs;
      totalFastMs += fastMs;
      totalCertifiedPruneRatio += certified.pruneRatio();
      totalFastPruneRatio += fast.pruneRatio();
      totalFastPrecision += fastPrecisionAtK;
      totalPartitionedShuffleWrite += partitioned.shuffleWriteBytes();
      totalPartitionedCommunicationReduction += partitioned.communicationReduction();
      totalPartitionedPrecision += partitionedPrecisionAtK;
      queryCount++;

      System.out.printf(
          "query=%s exactMs=%.3f certifiedPrunedMs=%.3f fastCandidateMs=%.3f "
              + "candidates=%d certifiedRefined=%d fastRefined=%d certifiedPruned=%d fastPruned=%d "
              + "certifiedPruneRatio=%.3f fastPruneRatio=%.3f candidateCommunicationReduction=%.3f "
              + "partitionedRefined=%d partitionedPruned=%d partitionedShuffleWriteProxyBytes=%d "
              + "partitionedCommunicationReduction=%.3f topKAgreement=%s fastPrecisionAtK=%.3f "
              + "partitionedPrecisionAtK=%.3f%n",
          query.getKey(),
          exactMs,
          certifiedMs,
          fastMs,
          certified.objectCount(),
          certified.refinedCount(),
          fast.refinedCount(),
          certified.prunedCount(),
          fast.prunedCount(),
          certified.pruneRatio(),
          fast.pruneRatio(),
          fast.pruneRatio(),
          partitioned.refinedCount(),
          partitioned.prunedCount(),
          partitioned.shuffleWriteBytes(),
          partitioned.communicationReduction(),
          certifiedAgrees,
          fastPrecisionAtK,
          partitionedPrecisionAtK);
      System.out.println("topK=" + certified.topK());
    }
    if (queryCount > 0) {
      System.out.printf(
          "summary avgExactMs=%.3f avgCertifiedPrunedMs=%.3f avgFastCandidateMs=%.3f "
              + "avgCertifiedPruneRatio=%.3f avgFastPruneRatio=%.3f avgCandidateCommunicationReduction=%.3f "
              + "avgFastPrecisionAtK=%.3f avgPartitionedShuffleWriteProxyBytes=%.0f "
              + "avgPartitionedCommunicationReduction=%.3f avgPartitionedPrecisionAtK=%.3f%n",
          totalExactMs / queryCount,
          totalCertifiedMs / queryCount,
          totalFastMs / queryCount,
          totalCertifiedPruneRatio / queryCount,
          totalFastPruneRatio / queryCount,
          totalFastPruneRatio / queryCount,
          totalFastPrecision / queryCount,
          totalPartitionedShuffleWrite / queryCount,
          totalPartitionedCommunicationReduction / queryCount,
          totalPartitionedPrecision / queryCount);
    }
  }

  private static PartitionedRanking partitionedCandidateRanking(
      List<ProbabilisticInstance> instances,
      QueryPoint queryPoint,
      int k,
      int partitions,
      int candidateMultiplier) {
    Map<Integer, List<ProbabilisticInstance>> byPartition = instances.stream()
        .collect(Collectors.groupingBy(i -> Math.floorMod(i.objectId().hashCode(), Math.max(1, partitions))));
    Set<String> candidateIds = new HashSet<>();
    int objectCount = (int) instances.stream().map(ProbabilisticInstance::objectId).distinct().count();

    for (List<ProbabilisticInstance> partition : byPartition.values()) {
      int partitionObjects = (int) partition.stream().map(ProbabilisticInstance::objectId).distinct().count();
      int targetCandidates = (int) Math.ceil((double) k * candidateMultiplier / Math.max(1, partitions));
      int limit = Math.min(partitionObjects, Math.max(k, targetCandidates));
      ProbabilisticTopK.PrunedRanking local =
          ProbabilisticTopK.candidatePrunedTopK(partition, queryPoint, limit, limit);
      local.topK().stream().map(CandidateScore::objectId).forEach(candidateIds::add);
    }

    List<ProbabilisticInstance> refinedInstances = instances.stream()
        .filter(i -> candidateIds.contains(i.objectId()))
        .toList();
    int refined = candidateIds.size();
    List<CandidateScore> topK = refinedInstances.isEmpty()
        ? List.of()
        : ProbabilisticTopK.exactTopKForObjects(refinedInstances, instances, queryPoint, k);
    return new PartitionedRanking(topK, objectCount, refined, objectCount - refined, refined * 128L);
  }

  private static double precisionAtK(List<String> exact, List<String> candidate) {
    if (exact.isEmpty()) {
      return 1.0;
    }
    long hits = candidate.stream().filter(exact::contains).count();
    return (double) hits / exact.size();
  }

  private static DataSplit holdoutSplit(List<RawEvent> events) {
    List<RawEvent> training = new ArrayList<>();
    List<RawEvent> holdout = new ArrayList<>();
    Map<String, Integer> seenByQuery = new java.util.LinkedHashMap<>();
    for (RawEvent event : events) {
      int index = seenByQuery.merge(event.queryId(), 1, Integer::sum);
      if (index % 5 == 0) {
        holdout.add(event);
      } else {
        training.add(event);
      }
    }
    if (training.isEmpty()) {
      training.addAll(events);
    }
    return new DataSplit(List.copyOf(training), List.copyOf(holdout));
  }

  private record PartitionedRanking(
      List<CandidateScore> topK,
      int objectCount,
      int refinedCount,
      int prunedCount,
      long shuffleWriteBytes) {
    double communicationReduction() {
      return objectCount == 0 ? 0.0 : (double) prunedCount / objectCount;
    }
  }

  private record DataSplit(
      List<RawEvent> training,
      List<RawEvent> holdout) {
  }
}
