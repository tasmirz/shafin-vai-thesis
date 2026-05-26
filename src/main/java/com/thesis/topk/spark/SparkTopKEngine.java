package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.DominanceScorer;
import com.thesis.topk.algorithm.ImputationEngine;
import com.thesis.topk.algorithm.ProbabilisticTopK;
import com.thesis.topk.algorithm.index.AggregateRTree;
import com.thesis.topk.algorithm.index.AggregateRTree.Inspection;
import com.thesis.topk.algorithm.index.AggregateRTree.LevelSelection;
import com.thesis.topk.algorithm.index.AggregateRTree.NodeReference;
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
 * refinement, and only surviving object groups are refined exactly. Curated uncertain-object
 * datasets use one aggregate R-tree per server partition: selected exported index levels provide
 * DDR/MBR score bounds, and partial MBR references are traversed in a reducer-shaped Spark stage.
 * Explicit treatments select whether DSCP filtering and AES-style emission accounting are active
 * so one pipeline can execute the paper control and its three ablation variants.</p>
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
    SparkTaskMetrics observedMetrics = new SparkTaskMetrics();
    sc.sc().addSparkListener(observedMetrics);
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
      rankings.add(rankQuery(
          sc, instances, queryPoint, k, targetPartitions, algorithm, validateExact, observedMetrics));
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
      boolean validateExact,
      SparkTaskMetrics observedMetrics) {
    JavaRDD<ProbabilisticInstance> queryInstances = allInstances
        .filter(instance -> instance.queryId().equals(queryPoint.queryId()))
        .persist(StorageLevel.MEMORY_ONLY());

    long queryInstanceCount = queryInstances.count();
    if (queryInstanceCount == 0) {
      queryInstances.unpersist();
      return new QueryRanking(
          queryPoint.queryId(), algorithm.id(), algorithm.dscpEnabled(), algorithm.aesEnabled(),
          new ArrayList<>(), 0, 0, 0, Double.NaN, 0L, 0L, 0L, 0L,
          validateExact, true, 0L, 0L, 0L, 0L,
          new SparkTaskMetrics.Snapshot(0, 0, 0, 0, 0, 0, 0, 0), false);
    }

    SparkTaskMetrics.Snapshot metricsBefore = observedMetrics.snapshot();
    JavaPairRDD<Integer, Iterable<ProbabilisticInstance>> groupedByPartition = queryInstances
        .mapToPair(instance -> new Tuple2<>(serverPartition(instance, partitions), instance))
        .groupByKey(partitions)
        .persist(StorageLevel.MEMORY_ONLY());
    boolean indexedMbrPath = queryInstances.filter(instance -> !instance.hasMbr()).take(1).isEmpty();
    long filteringStart = System.nanoTime();
    List<ProbabilisticInstance> allForQuery = validateExact || !indexedMbrPath
        ? queryInstances.collect()
        : List.of();
    Broadcast<List<ProbabilisticInstance>> allBroadcast = indexedMbrPath
        ? null
        : sc.broadcast(allForQuery);
    Broadcast<Map<Integer, AggregateRTree>> indexBroadcast = null;

    JavaRDD<CandidateEnvelope> roughCandidates;
    if (indexedMbrPath) {
      Map<Integer, AggregateRTree> indexes = groupedByPartition
          .mapToPair(tuple -> new Tuple2<>(
              tuple._1(), AggregateRTree.build(
                  tuple._1(), iterableToList(tuple._2()), AggregateRTree.DEFAULT_FANOUT)))
          .collectAsMap();
      indexBroadcast = sc.broadcast(new HashMap<>(indexes));
      Broadcast<Map<Integer, AggregateRTree>> indexesBroadcast = indexBroadcast;
      roughCandidates = groupedByPartition
          .flatMap(tuple -> indexedPartitionEnvelopes(
              tuple._1(), queryPoint, iterableToList(tuple._2()), indexesBroadcast.value()).iterator())
          .persist(StorageLevel.MEMORY_ONLY());
    } else {
      Broadcast<List<ProbabilisticInstance>> instancesBroadcast = allBroadcast;
      roughCandidates = groupedByPartition
          .flatMap(tuple -> partitionEnvelopes(
              tuple._1(), queryPoint, iterableToList(tuple._2()), instancesBroadcast.value(), partitions).iterator())
          .persist(StorageLevel.MEMORY_ONLY());
    }

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
    long filteringNanos = System.nanoTime() - filteringStart;
    long baselineEmissions = survivors.map(CandidateEnvelope::baselineEmissions).fold(0L, Long::sum);
    long aesEmissions = survivors.map(CandidateEnvelope::aesEmissions).fold(0L, Long::sum);
    long emissionStart = System.nanoTime();
    JavaPairRDD<String, String> emissions = null;
    JavaPairRDD<Integer, PartialMbrWork> partialEmissions = null;
    long emittedRecords;
    Broadcast<Map<String, Double>> partialScoresBroadcast = null;
    if (indexedMbrPath) {
      partialEmissions = survivors
          .flatMapToPair(candidate -> emitPartialMbrRecords(candidate, algorithm).iterator())
          .partitionBy(new HashPartitioner(partitions))
          .persist(StorageLevel.MEMORY_ONLY());
      emittedRecords = partialEmissions.count();
      Broadcast<Map<Integer, AggregateRTree>> indexesBroadcast = indexBroadcast;
      Map<String, Double> partialScores = partialEmissions
          .mapToPair(tuple -> new Tuple2<>(
              tuple._2().objectId(),
              tuple._2().exactContribution(indexesBroadcast.value().get(tuple._1()), queryPoint)))
          .reduceByKey(Double::sum)
          .collectAsMap();
      partialScoresBroadcast = sc.broadcast(new HashMap<>(partialScores));
    } else {
      emissions = survivors
          .flatMapToPair(candidate -> emitRecords(candidate, algorithm).iterator())
          .partitionBy(new HashPartitioner(partitions))
          .persist(StorageLevel.MEMORY_ONLY());
      emittedRecords = emissions.count();
    }
    long emissionNanos = System.nanoTime() - emissionStart;

    long refinementStart = System.nanoTime();
    JavaRDD<CandidateScore> refined;
    if (indexedMbrPath) {
      Broadcast<Map<String, Double>> scoresBroadcast = partialScoresBroadcast;
      refined = survivors
          .map(candidate -> new CandidateScore(
              candidate.objectId(),
              queryPoint.queryId(),
              candidate.lowerBound()
                  + scoresBroadcast.value().getOrDefault(candidate.objectId(), 0.0),
              candidate.lowerBound(),
              candidate.upperBound(),
              candidate.instances().size()))
          .persist(StorageLevel.MEMORY_ONLY());
    } else {
      Broadcast<List<ProbabilisticInstance>> instancesBroadcast = allBroadcast;
      refined = survivors
          .map(candidate -> {
            double exact = DominanceScorer.expectedDominanceScore(
                candidate.instances(), instancesBroadcast.value(), queryPoint);
            return new CandidateScore(
                candidate.objectId(),
                queryPoint.queryId(),
                exact,
                candidate.lowerBound(),
                candidate.upperBound(),
                candidate.instances().size());
          })
          .persist(StorageLevel.MEMORY_ONLY());
    }

    refined.count();
    List<CandidateScore> topK = refined.takeOrdered(k);
    long refinementNanos = System.nanoTime() - refinementStart;
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
    if (emissions != null) {
      emissions.unpersist();
    }
    if (partialEmissions != null) {
      partialEmissions.unpersist();
    }
    survivors.unpersist();
    roughCandidates.unpersist();
    groupedByPartition.unpersist();
    queryInstances.unpersist();
    if (thresholdBroadcast != null) {
      thresholdBroadcast.destroy();
    }
    if (partialScoresBroadcast != null) {
      partialScoresBroadcast.destroy();
    }
    if (indexBroadcast != null) {
      indexBroadcast.destroy();
    }
    if (allBroadcast != null) {
      allBroadcast.destroy();
    }
    SparkTaskMetrics.Snapshot observed = observedMetrics.delta(metricsBefore);

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
        validationNanos,
        filteringNanos,
        emissionNanos,
        refinementNanos,
        observed,
        indexedMbrPath);
  }

  private static List<CandidateEnvelope> indexedPartitionEnvelopes(
      int partitionId,
      QueryPoint queryPoint,
      List<ProbabilisticInstance> localInstances,
      Map<Integer, AggregateRTree> indexes) {
    Map<String, List<ProbabilisticInstance>> localObjects = localInstances.stream()
        .collect(Collectors.groupingBy(
            ProbabilisticInstance::objectId, LinkedHashMap::new, Collectors.toList()));
    Map<Integer, LevelSelection> exportedLevels = new HashMap<>();
    AggregateRTree localIndex = indexes.get(partitionId);
    if (localIndex == null) {
      throw new IllegalArgumentException("No local aR-tree for partition " + partitionId);
    }
    for (Map.Entry<Integer, AggregateRTree> entry : indexes.entrySet()) {
      if (entry.getKey() != partitionId) {
        exportedLevels.put(
            entry.getKey(), entry.getValue().selectExportLevel(localInstances, queryPoint));
      }
    }
    List<CandidateEnvelope> envelopes = new ArrayList<>();
    for (Map.Entry<String, List<ProbabilisticInstance>> entry : localObjects.entrySet()) {
      double lowerBound = 0.0;
      double upperBound = 0.0;
      List<PartialMbrWork> partialWork = new ArrayList<>();
      for (ProbabilisticInstance candidate : entry.getValue()) {
        double localMass = localIndex.exactDominatedMass(candidate, queryPoint);
        double instanceLower = localMass;
        double instanceUpper = localMass;
        for (Map.Entry<Integer, LevelSelection> selection : exportedLevels.entrySet()) {
          Inspection inspection = indexes.get(selection.getKey())
              .inspectAtLevel(candidate, queryPoint, selection.getValue().level());
          instanceLower += inspection.fullyDominatedMass();
          instanceUpper += inspection.fullyDominatedMass() + inspection.partialUpperMass();
          if (!inspection.partiallyDominatedNodes().isEmpty()) {
            partialWork.add(new PartialMbrWork(
                entry.getKey(), candidate, inspection.partiallyDominatedNodes()));
          }
        }
        lowerBound += candidate.probability() * instanceLower;
        upperBound += candidate.probability() * instanceUpper;
      }
      envelopes.add(new CandidateEnvelope(
          partitionId, entry.getKey(), entry.getValue(), List.of(),
          lowerBound, upperBound, partialWork));
    }
    return envelopes;
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

      double lowerBound;
      double upperBound;
      // Stream inputs without MBR metadata retain a safe but intentionally loose remote bound.
      lowerBound = DominanceScorer.expectedDominanceScore(
          objectInstances, localInstances, queryPoint);
      double objectMass = objectInstances.stream()
          .mapToDouble(ProbabilisticInstance::probability)
          .sum();
      double remoteMass = allInstances.stream()
          .filter(other -> !other.objectId().equals(objectId))
          .filter(other -> serverPartition(other, partitions) != partitionId)
          .mapToDouble(ProbabilisticInstance::probability)
          .sum();
      upperBound = lowerBound + objectMass * remoteMass;
      envelopes.add(new CandidateEnvelope(
          partitionId, objectId, objectInstances, competitors, lowerBound, upperBound, List.of()));
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

  private static int serverPartition(ProbabilisticInstance instance, int partitions) {
    if (instance.serverPartition() >= 0) {
      return Math.floorMod(instance.serverPartition(), partitions);
    }
    String objectId = instance.objectId();
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

  private static List<Tuple2<Integer, PartialMbrWork>> emitPartialMbrRecords(
      CandidateEnvelope candidate,
      PtdAlgorithm algorithm) {
    List<Tuple2<Integer, PartialMbrWork>> records = new ArrayList<>();
    for (PartialMbrWork work : candidate.partialWork()) {
      if (algorithm.aesEnabled()) {
        records.add(new Tuple2<>(work.partitionId(), work));
      } else {
        for (NodeReference reference : work.references()) {
          List<NodeReference> singleReference = new ArrayList<>();
          singleReference.add(reference);
          records.add(new Tuple2<>(
              reference.partitionId(),
              new PartialMbrWork(work.objectId(), work.candidate(), singleReference)));
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
    private final List<PartialMbrWork> partialWork;

    CandidateEnvelope(
        int partitionId,
        String objectId,
        List<ProbabilisticInstance> instances,
        List<String> competitorIds,
        double lowerBound,
        double upperBound,
        List<PartialMbrWork> partialWork) {
      this.partitionId = partitionId;
      this.objectId = objectId;
      this.instances = new ArrayList<>(instances);
      this.competitorIds = new ArrayList<>(competitorIds);
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.partialWork = new ArrayList<>(partialWork);
    }

    int partitionId() { return partitionId; }
    String objectId() { return objectId; }
    List<ProbabilisticInstance> instances() { return instances; }
    List<String> competitorIds() { return competitorIds; }
    double lowerBound() { return lowerBound; }
    double upperBound() { return upperBound; }
    List<PartialMbrWork> partialWork() { return partialWork; }
    long baselineEmissions() {
      if (!partialWork.isEmpty()) {
        return partialWork.stream().mapToLong(work -> work.references().size()).sum();
      }
      return (long) instances.size() * competitorIds.size();
    }
    long aesEmissions() {
      if (!partialWork.isEmpty()) {
        return partialWork.size();
      }
      return competitorIds.isEmpty() ? 0L : instances.size();
    }
  }

  private static final class PartialMbrWork implements Serializable {
    private final String objectId;
    private final ProbabilisticInstance candidate;
    private final List<NodeReference> references;

    PartialMbrWork(
        String objectId,
        ProbabilisticInstance candidate,
        List<NodeReference> references) {
      this.objectId = objectId;
      this.candidate = candidate;
      this.references = new ArrayList<>(references);
    }

    String objectId() { return objectId; }
    ProbabilisticInstance candidate() { return candidate; }
    List<NodeReference> references() { return references; }

    int partitionId() {
      return references.get(0).partitionId();
    }

    double exactContribution(AggregateRTree index, QueryPoint queryPoint) {
      if (index == null) {
        throw new IllegalArgumentException("No aR-tree for reducer partition " + partitionId());
      }
      double dominatedMass = references.stream()
          .mapToDouble(reference -> index.exactDominatedMass(candidate, reference, queryPoint))
          .sum();
      return candidate.probability() * dominatedMass;
    }
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
    private final long filteringNanos;
    private final long emissionNanos;
    private final long refinementNanos;
    private final SparkTaskMetrics.Snapshot observedMetrics;
    private final boolean indexedMbrPath;

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
          compactShuffleRecords, 0L, validationPerformed, exactAgreement, validationNanos,
          0L, 0L, 0L, new SparkTaskMetrics.Snapshot(0, 0, 0, 0, 0, 0, 0, 0), false);
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
      this(queryId, algorithmId, dscpEnabled, aesEnabled, topK, objectCount, refinedCount,
          prunedCount, pruningThreshold, compactShuffleRecords, baselineEmissions, aesEmissions,
          falsePruneCount, validationPerformed, exactAgreement, validationNanos, 0L, 0L, 0L,
          new SparkTaskMetrics.Snapshot(0, 0, 0, 0, 0, 0, 0, 0), false);
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
        long validationNanos,
        long filteringNanos,
        long emissionNanos,
        long refinementNanos,
        SparkTaskMetrics.Snapshot observedMetrics,
        boolean indexedMbrPath) {
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
      this.filteringNanos = filteringNanos;
      this.emissionNanos = emissionNanos;
      this.refinementNanos = refinementNanos;
      this.observedMetrics = observedMetrics;
      this.indexedMbrPath = indexedMbrPath;
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
    public long filteringNanos() { return filteringNanos; }
    public long emissionNanos() { return emissionNanos; }
    public long refinementNanos() { return refinementNanos; }
    public SparkTaskMetrics.Snapshot observedMetrics() { return observedMetrics; }
    public boolean indexedMbrPath() { return indexedMbrPath; }

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
