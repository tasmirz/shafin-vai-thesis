package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.DominanceScorer;
import com.thesis.topk.algorithm.ImputationEngine;
import com.thesis.topk.algorithm.ProbabilisticTopK;
import com.thesis.topk.algorithm.variant.PtdAlgorithm;
import com.thesis.topk.algorithm.variant.PtdAlgorithmRegistry;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.RawEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.HashPartitioner;
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
 * events are expanded into probabilistic instances, objects are assigned to server partitions,
 * conservative partition-local lower/upper bounds are computed without performing global exact
 * refinement, and only surviving object groups are refined exactly.
 * Explicit treatments select whether DSCP filtering and AES-style emission accounting are active
 * so one pipeline can execute the paper control and its three ablation variants. MBR/aR-tree
 * bounds are intentionally left to the paper-shaped spatial dataset adapter.</p>
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
    return rank(sc, events, queryPoints, synopsis, k, partitions,
        PtdAlgorithmRegistry.defaultAlgorithm(), false);
  }

  public static SparkRunResult rank(
      JavaSparkContext sc,
      List<RawEvent> events,
      Map<String, QueryPoint> queryPoints,
      DdImputationSynopsis synopsis,
      int k,
      int partitions,
      boolean validateExact) {
    return rank(sc, events, queryPoints, synopsis, k, partitions,
        PtdAlgorithmRegistry.defaultAlgorithm(), validateExact);
  }

  public static SparkRunResult rank(
      JavaSparkContext sc,
      List<RawEvent> events,
      Map<String, QueryPoint> queryPoints,
      DdImputationSynopsis synopsis,
      int k,
      int partitions,
      PtdAlgorithm algorithm,
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
      rankings.add(rankQuery(sc, instances, queryPoint, k, targetPartitions, algorithm, validateExact));
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
      PtdAlgorithm algorithm,
      boolean validateExact) {
    JavaRDD<ProbabilisticInstance> queryInstances = allInstances
        .filter(instance -> instance.queryId().equals(queryPoint.queryId()))
        .persist(StorageLevel.MEMORY_ONLY());

    long queryInstanceCount = queryInstances.count();
    if (queryInstanceCount == 0) {
      queryInstances.unpersist();
      return new QueryRanking(
          queryPoint.queryId(), algorithm.id(), algorithm.dscpEnabled(), algorithm.aesEnabled(),
          new ArrayList<>(), 0, 0, 0, Double.NaN, 0L, 0L, 0L, 0L,
          validateExact, true, 0L);
    }

    List<ProbabilisticInstance> allForQuery = queryInstances.collect();
    Broadcast<List<ProbabilisticInstance>> allBroadcast = sc.broadcast(allForQuery);

    JavaPairRDD<Integer, Iterable<ProbabilisticInstance>> groupedByPartition = queryInstances
        .mapToPair(instance -> new Tuple2<>(serverPartition(instance.objectId(), partitions), instance))
        .groupByKey(partitions)
        .persist(StorageLevel.MEMORY_ONLY());

    JavaRDD<CandidateEnvelope> roughCandidates = groupedByPartition
        .flatMap(tuple -> partitionEnvelopes(
            tuple._1(), queryPoint, iterableToList(tuple._2()), allBroadcast.value(), partitions).iterator())
        .persist(StorageLevel.MEMORY_ONLY());

    long objectCount = roughCandidates.count();
    double tau = Double.NaN;
    Broadcast<Map<Integer, Double>> thresholdBroadcast = null;
    JavaRDD<CandidateEnvelope> survivors = roughCandidates;
    if (algorithm.dscpEnabled()) {
      Map<Integer, Double> thresholds = roughCandidates
          .mapToPair(candidate -> new Tuple2<>(candidate.partitionId(), candidate.lowerBound()))
          .groupByKey(partitions)
          .mapValues(values -> kthLowerBound(iterableToList(values), k))
          .collectAsMap();
      tau = thresholds.values().stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
      thresholdBroadcast = sc.broadcast(new HashMap<>(thresholds));
      Broadcast<Map<Integer, Double>> thresholdsBroadcast = thresholdBroadcast;
      survivors = roughCandidates.filter(
          candidate -> candidate.upperBound() >= thresholdsBroadcast.value().get(candidate.partitionId()));
    }
    survivors = survivors.persist(StorageLevel.MEMORY_ONLY());

    long refinedCount = survivors.count();
    long baselineEmissions = survivors
        .map(candidate -> (long) candidate.instances().size() * candidate.competitorIds().size())
        .fold(0L, Long::sum);
    long aesEmissions = survivors
        .filter(candidate -> !candidate.competitorIds().isEmpty())
        .map(candidate -> (long) candidate.instances().size())
        .fold(0L, Long::sum);
    JavaPairRDD<String, String> emissions = survivors
        .flatMapToPair(candidate -> emitRecords(candidate, algorithm).iterator())
        .partitionBy(new HashPartitioner(partitions))
        .persist(StorageLevel.MEMORY_ONLY());
    long emittedRecords = emissions.count();

    JavaRDD<CandidateScore> refined = survivors
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

    refined.count();
    List<CandidateScore> topK = refined.takeOrdered(k);
    boolean exactAgreement = true;
    long validationNanos = 0L;
    long falsePruneCount = 0L;
    if (validateExact) {
      long validationStart = System.nanoTime();
      List<String> exactIds = ProbabilisticTopK.exactTopK(allForQuery, queryPoint, k).stream()
          .map(CandidateScore::objectId)
          .toList();
      exactAgreement = topK.stream().map(CandidateScore::objectId).toList().equals(exactIds);
      if (algorithm.dscpEnabled()) {
        List<String> survivorIds = survivors.map(CandidateEnvelope::objectId).collect();
        falsePruneCount = exactIds.stream().filter(id -> !survivorIds.contains(id)).count();
      }
      validationNanos = System.nanoTime() - validationStart;
    }

    long prunedCount = Math.max(0L, objectCount - refinedCount);

    refined.unpersist();
    emissions.unpersist();
    survivors.unpersist();
    roughCandidates.unpersist();
    groupedByPartition.unpersist();
    queryInstances.unpersist();
    if (thresholdBroadcast != null) {
      thresholdBroadcast.destroy();
    }
    allBroadcast.destroy();

    return new QueryRanking(
        queryPoint.queryId(),
        algorithm.id(),
        algorithm.dscpEnabled(),
        algorithm.aesEnabled(),
        topK,
        objectCount,
        refinedCount,
        prunedCount,
        tau,
        emittedRecords,
        baselineEmissions,
        aesEmissions,
        falsePruneCount,
        validateExact,
        exactAgreement,
        validationNanos);
  }

  private static List<CandidateEnvelope> partitionEnvelopes(
      int partitionId,
      QueryPoint queryPoint,
      List<ProbabilisticInstance> localInstances,
      List<ProbabilisticInstance> allInstances,
      int partitions) {
    Map<String, List<ProbabilisticInstance>> localObjects = localInstances.stream()
        .collect(Collectors.groupingBy(
            ProbabilisticInstance::objectId, LinkedHashMap::new, Collectors.toList()));
    List<String> localIds = new ArrayList<>(localObjects.keySet());
    List<CandidateEnvelope> envelopes = new ArrayList<>();
    for (Map.Entry<String, List<ProbabilisticInstance>> entry : localObjects.entrySet()) {
      String objectId = entry.getKey();
      List<ProbabilisticInstance> objectInstances = entry.getValue();
      List<String> competitors = localIds.stream().filter(id -> !id.equals(objectId)).toList();

      // Local exact domination mass is a lower bound on the global score. Until MBR/aR-tree
      // summaries are connected, all remote mass is retained as a conservative upper allowance.
      double lowerBound = DominanceScorer.expectedDominanceScore(
          objectInstances, localInstances, queryPoint);
      double objectMass = objectInstances.stream()
          .mapToDouble(ProbabilisticInstance::probability)
          .sum();
      double remoteMass = allInstances.stream()
          .filter(other -> !other.objectId().equals(objectId))
          .filter(other -> serverPartition(other.objectId(), partitions) != partitionId)
          .mapToDouble(ProbabilisticInstance::probability)
          .sum();
      double upperBound = lowerBound + objectMass * remoteMass;
      envelopes.add(new CandidateEnvelope(
          partitionId, objectId, objectInstances, competitors, lowerBound, upperBound));
    }
    return envelopes;
  }

  private static double kthLowerBound(List<Double> lowerBounds, int k) {
    return lowerBounds.stream()
        .sorted(Comparator.reverseOrder())
        .skip(Math.max(0, k - 1L))
        .findFirst()
        .orElse(Double.NEGATIVE_INFINITY);
  }

  private static <T> List<T> iterableToList(Iterable<T> iterable) {
    List<T> values = new ArrayList<>();
    iterable.forEach(values::add);
    return values;
  }

  private static int serverPartition(String objectId, int partitions) {
    return Math.floorMod(objectId.hashCode(), partitions);
  }

  private static List<Tuple2<String, String>> emitRecords(
      CandidateEnvelope candidate,
      PtdAlgorithm algorithm) {
    List<Tuple2<String, String>> records = new ArrayList<>();
    if (candidate.competitorIds().isEmpty()) {
      return records;
    }
    for (ProbabilisticInstance instance : candidate.instances()) {
      if (algorithm.aesEnabled()) {
        records.add(new Tuple2<>(
            Integer.toString(candidate.partitionId()),
            instance.instanceId() + "|" + String.join(";", candidate.competitorIds())));
      } else {
        for (String competitor : candidate.competitorIds()) {
          records.add(new Tuple2<>(
              Integer.toString(candidate.partitionId()), instance.instanceId() + "|" + competitor));
        }
      }
    }
    return records;
  }

  private static final class CandidateEnvelope implements Serializable {
    private final int partitionId;
    private final String objectId;
    private final List<ProbabilisticInstance> instances;
    private final List<String> competitorIds;
    private final double lowerBound;
    private final double upperBound;

    CandidateEnvelope(
        int partitionId,
        String objectId,
        List<ProbabilisticInstance> instances,
        List<String> competitorIds,
        double lowerBound,
        double upperBound) {
      this.partitionId = partitionId;
      this.objectId = objectId;
      this.instances = instances;
      this.competitorIds = competitorIds;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    int partitionId() { return partitionId; }
    String objectId() { return objectId; }
    List<ProbabilisticInstance> instances() { return instances; }
    List<String> competitorIds() { return competitorIds; }
    double lowerBound() { return lowerBound; }
    double upperBound() { return upperBound; }
  }

  public static final class QueryRanking implements Serializable {
    private final String queryId;
    private final String algorithmId;
    private final boolean dscpEnabled;
    private final boolean aesEnabled;
    private final List<CandidateScore> topK;
    private final long objectCount;
    private final long refinedCount;
    private final long prunedCount;
    private final double pruningThreshold;
    private final long compactShuffleRecords;
    private final long baselineEmissions;
    private final long aesEmissions;
    private final long falsePruneCount;
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
      this(queryId, PtdAlgorithmRegistry.DEFAULT_ID, true, true, topK, objectCount, refinedCount,
          prunedCount, pruningThreshold, compactShuffleRecords, compactShuffleRecords,
          compactShuffleRecords, 0L, validationPerformed, exactAgreement, validationNanos);
    }

    public QueryRanking(
        String queryId,
        String algorithmId,
        boolean dscpEnabled,
        boolean aesEnabled,
        List<CandidateScore> topK,
        long objectCount,
        long refinedCount,
        long prunedCount,
        double pruningThreshold,
        long compactShuffleRecords,
        long baselineEmissions,
        long aesEmissions,
        long falsePruneCount,
        boolean validationPerformed,
        boolean exactAgreement,
        long validationNanos) {
      this.queryId = queryId;
      this.algorithmId = algorithmId;
      this.dscpEnabled = dscpEnabled;
      this.aesEnabled = aesEnabled;
      this.topK = topK;
      this.objectCount = objectCount;
      this.refinedCount = refinedCount;
      this.prunedCount = prunedCount;
      this.pruningThreshold = pruningThreshold;
      this.compactShuffleRecords = compactShuffleRecords;
      this.baselineEmissions = baselineEmissions;
      this.aesEmissions = aesEmissions;
      this.falsePruneCount = falsePruneCount;
      this.validationPerformed = validationPerformed;
      this.exactAgreement = exactAgreement;
      this.validationNanos = validationNanos;
    }

    public String queryId() { return queryId; }
    public String algorithmId() { return algorithmId; }
    public boolean dscpEnabled() { return dscpEnabled; }
    public boolean aesEnabled() { return aesEnabled; }
    public List<CandidateScore> topK() { return topK; }
    public long objectCount() { return objectCount; }
    public long refinedCount() { return refinedCount; }
    public long prunedCount() { return prunedCount; }
    public double pruningThreshold() { return pruningThreshold; }
    public long compactShuffleRecords() { return compactShuffleRecords; }
    public long emittedRecords() { return compactShuffleRecords; }
    public long baselineEmissions() { return baselineEmissions; }
    public long aesEmissions() { return aesEmissions; }
    public long falsePruneCount() { return falsePruneCount; }
    public boolean validationPerformed() { return validationPerformed; }
    public boolean exactAgreement() { return exactAgreement; }
    public long validationNanos() { return validationNanos; }

    public double pruneRatio() {
      return objectCount == 0L ? 0.0 : (double) prunedCount / objectCount;
    }

    public double aggregatedEmissionRate() {
      return baselineEmissions == 0L ? 0.0 : (double) aesEmissions / baselineEmissions;
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
