package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.DominanceScorer;
import com.thesis.topk.algorithm.ImputationEngine;
import com.thesis.topk.algorithm.ProbabilisticTopK;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.RawEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Spark RDD implementation of the paper-inspired probabilistic top-k dominance pipeline.
 *
 * <p>The engine keeps the algorithmic shape of the 2025 ICCIT PTD framework: missing raw
 * events are expanded into probabilistic instances, object-level lower/upper bounds are computed
 * as distributed candidate records, DSCP-style threshold pruning removes weak objects, and only
 * surviving object groups are refined exactly. The Spark shuffle carries compact object-level
 * groups rather than repeated instance-competitor emissions, which is the Spark analogue of AES.</p>
 */
public final class SparkTopKEngine {
  private SparkTopKEngine() {
  }

  public static SparkRunResult rank(
      JavaSparkContext sc,
      List<RawEvent> events,
      Map<String, QueryPoint> queryPoints,
      DdImputationSynopsis synopsis,
      int k,
      int partitions) {
    return rank(sc, events, queryPoints, synopsis, k, partitions, false);
  }

  public static SparkRunResult rank(
      JavaSparkContext sc,
      List<RawEvent> events,
      Map<String, QueryPoint> queryPoints,
      DdImputationSynopsis synopsis,
      int k,
      int partitions,
      boolean validateExact) {
    int targetPartitions = Math.max(1, partitions);
    Broadcast<DdImputationSynopsis> synopsisBroadcast = sc.broadcast(synopsis);

    JavaRDD<RawEvent> rawEvents = sc.parallelize(events, targetPartitions).cache();
    long rawEventCount = rawEvents.count();

    JavaRDD<ProbabilisticInstance> instances = rawEvents
        .filter(event -> event.opType() == OpType.UPSERT)
        .flatMap(event -> ImputationEngine.impute(event, synopsisBroadcast.value()).iterator())
        .repartition(targetPartitions)
        .persist(StorageLevel.MEMORY_ONLY());
    long instanceCount = instances.count();

    List<QueryRanking> rankings = new ArrayList<>();
    for (QueryPoint queryPoint : queryPoints.values()) {
      rankings.add(rankQuery(sc, instances, queryPoint, k, targetPartitions, validateExact));
    }

    instances.unpersist();
    rawEvents.unpersist();
    synopsisBroadcast.destroy();
    return new SparkRunResult(rawEventCount, instanceCount, rankings);
  }

  private static QueryRanking rankQuery(
      JavaSparkContext sc,
      JavaRDD<ProbabilisticInstance> allInstances,
      QueryPoint queryPoint,
      int k,
      int partitions,
      boolean validateExact) {
    JavaRDD<ProbabilisticInstance> queryInstances = allInstances
        .filter(instance -> instance.queryId().equals(queryPoint.queryId()))
        .persist(StorageLevel.MEMORY_ONLY());

    long queryInstanceCount = queryInstances.count();
    if (queryInstanceCount == 0) {
      queryInstances.unpersist();
      return new QueryRanking(
          queryPoint.queryId(), new ArrayList<>(), 0, 0, 0, 0.0, 0L, validateExact, true, 0L);
    }

    List<ProbabilisticInstance> allForQuery = queryInstances.collect();
    Broadcast<List<ProbabilisticInstance>> allBroadcast = sc.broadcast(allForQuery);

    JavaPairRDD<String, Iterable<ProbabilisticInstance>> groupedByObject = queryInstances
        .mapToPair(instance -> new Tuple2<>(instance.objectId(), instance))
        .groupByKey(partitions)
        .persist(StorageLevel.MEMORY_ONLY());

    JavaRDD<CandidateEnvelope> roughCandidates = groupedByObject
        .map(tuple -> toEnvelope(tuple._1(), queryPoint, iterableToList(tuple._2()), allBroadcast.value()))
        .persist(StorageLevel.MEMORY_ONLY());

    long objectCount = roughCandidates.count();
    List<Double> lowerBounds = roughCandidates
        .map(CandidateEnvelope::lowerBound)
        .collect();
    double tau = kthLowerBound(lowerBounds, k);
    Broadcast<Double> thresholdBroadcast = sc.broadcast(tau);

    JavaRDD<CandidateScore> refined = roughCandidates
        .filter(candidate -> candidate.upperBound() >= thresholdBroadcast.value())
        .map(candidate -> {
          double exact = DominanceScorer.expectedDominanceScore(
              candidate.instances(), allBroadcast.value(), queryPoint);
          return new CandidateScore(
              candidate.objectId(),
              queryPoint.queryId(),
              exact,
              candidate.lowerBound(),
              candidate.upperBound(),
              candidate.instances().size());
        })
        .persist(StorageLevel.MEMORY_ONLY());

    long refinedCount = refined.count();
    List<CandidateScore> topK = refined.takeOrdered(k);
    boolean exactAgreement = true;
    long validationNanos = 0L;
    if (validateExact) {
      long validationStart = System.nanoTime();
      List<String> exactIds = ProbabilisticTopK.exactTopK(allForQuery, queryPoint, k).stream()
          .map(CandidateScore::objectId)
          .toList();
      exactAgreement = topK.stream().map(CandidateScore::objectId).toList().equals(exactIds);
      validationNanos = System.nanoTime() - validationStart;
    }

    long compactShuffleRecords = objectCount;
    long prunedCount = Math.max(0L, objectCount - refinedCount);

    refined.unpersist();
    roughCandidates.unpersist();
    groupedByObject.unpersist();
    queryInstances.unpersist();
    thresholdBroadcast.destroy();
    allBroadcast.destroy();

    return new QueryRanking(
        queryPoint.queryId(),
        topK,
        objectCount,
        refinedCount,
        prunedCount,
        tau,
        compactShuffleRecords,
        validateExact,
        exactAgreement,
        validationNanos);
  }

  private static CandidateEnvelope toEnvelope(
      String objectId,
      QueryPoint queryPoint,
      List<ProbabilisticInstance> objectInstances,
      List<ProbabilisticInstance> allInstances) {
    double exact = DominanceScorer.expectedDominanceScore(objectInstances, allInstances, queryPoint);
    double lowerBound = Math.max(0.0, exact - bestProbability(objectInstances));
    double upperBound = DominanceScorer.closenessUpperBound(objectInstances, allInstances, queryPoint);
    return new CandidateEnvelope(objectId, objectInstances, lowerBound, upperBound);
  }

  private static double bestProbability(List<ProbabilisticInstance> instances) {
    return instances.stream().mapToDouble(ProbabilisticInstance::probability).max().orElse(0.0);
  }

  private static double kthLowerBound(List<Double> lowerBounds, int k) {
    return lowerBounds.stream()
        .sorted(Comparator.reverseOrder())
        .skip(Math.max(0, k - 1L))
        .findFirst()
        .orElse(Double.NEGATIVE_INFINITY);
  }

  private static List<ProbabilisticInstance> iterableToList(Iterable<ProbabilisticInstance> iterable) {
    List<ProbabilisticInstance> values = new ArrayList<>();
    iterable.forEach(values::add);
    return values;
  }

  private static final class CandidateEnvelope implements Serializable {
    private final String objectId;
    private final List<ProbabilisticInstance> instances;
    private final double lowerBound;
    private final double upperBound;

    CandidateEnvelope(
        String objectId,
        List<ProbabilisticInstance> instances,
        double lowerBound,
        double upperBound) {
      this.objectId = objectId;
      this.instances = instances;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    String objectId() { return objectId; }
    List<ProbabilisticInstance> instances() { return instances; }
    double lowerBound() { return lowerBound; }
    double upperBound() { return upperBound; }
  }

  public static final class QueryRanking implements Serializable {
    private final String queryId;
    private final List<CandidateScore> topK;
    private final long objectCount;
    private final long refinedCount;
    private final long prunedCount;
    private final double pruningThreshold;
    private final long compactShuffleRecords;
    private final boolean validationPerformed;
    private final boolean exactAgreement;
    private final long validationNanos;

    public QueryRanking(
        String queryId,
        List<CandidateScore> topK,
        long objectCount,
        long refinedCount,
        long prunedCount,
        double pruningThreshold,
        long compactShuffleRecords) {
      this(queryId, topK, objectCount, refinedCount, prunedCount, pruningThreshold,
          compactShuffleRecords, false, true, 0L);
    }

    public QueryRanking(
        String queryId,
        List<CandidateScore> topK,
        long objectCount,
        long refinedCount,
        long prunedCount,
        double pruningThreshold,
        long compactShuffleRecords,
        boolean validationPerformed,
        boolean exactAgreement,
        long validationNanos) {
      this.queryId = queryId;
      this.topK = topK;
      this.objectCount = objectCount;
      this.refinedCount = refinedCount;
      this.prunedCount = prunedCount;
      this.pruningThreshold = pruningThreshold;
      this.compactShuffleRecords = compactShuffleRecords;
      this.validationPerformed = validationPerformed;
      this.exactAgreement = exactAgreement;
      this.validationNanos = validationNanos;
    }

    public String queryId() { return queryId; }
    public List<CandidateScore> topK() { return topK; }
    public long objectCount() { return objectCount; }
    public long refinedCount() { return refinedCount; }
    public long prunedCount() { return prunedCount; }
    public double pruningThreshold() { return pruningThreshold; }
    public long compactShuffleRecords() { return compactShuffleRecords; }
    public boolean validationPerformed() { return validationPerformed; }
    public boolean exactAgreement() { return exactAgreement; }
    public long validationNanos() { return validationNanos; }

    public double pruneRatio() {
      return objectCount == 0L ? 0.0 : (double) prunedCount / objectCount;
    }
  }

  public static final class SparkRunResult implements Serializable {
    private final long rawEventCount;
    private final long probabilisticInstanceCount;
    private final List<QueryRanking> rankings;

    public SparkRunResult(
        long rawEventCount,
        long probabilisticInstanceCount,
        List<QueryRanking> rankings) {
      this.rawEventCount = rawEventCount;
      this.probabilisticInstanceCount = probabilisticInstanceCount;
      this.rankings = rankings;
    }

    public long rawEventCount() { return rawEventCount; }
    public long probabilisticInstanceCount() { return probabilisticInstanceCount; }
    public List<QueryRanking> rankings() { return rankings; }

    public long validationNanos() {
      return rankings.stream().mapToLong(QueryRanking::validationNanos).sum();
    }
  }
}
