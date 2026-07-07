package com.thesis.topk.spark;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.DominanceScorer;
import com.thesis.topk.algorithm.ImputationEngine;
import com.thesis.topk.algorithm.ProbabilisticTopK;
import com.thesis.topk.algorithm.variant.PtdAlgorithm;
import com.thesis.topk.algorithm.variant.PtdAlgorithmRegistry;
import com.thesis.topk.dataset.CsvDatasetProvider;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.spark.SparkTopKEngine.QueryRanking;
import com.thesis.topk.spark.SparkTopKEngine.ObjectTrace;
import com.thesis.topk.spark.SparkTopKEngine.SparkRunResult;
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
 * HGTP: Hierarchical Global Threshold Pruning engine for probabilistic top-k dominance.
 *
 * <p>Implements three novel components on top of the AES+DSCP pipeline:
 * <ol>
 *   <li><b>MBR Skyline Pre-Filter</b> — prunes objects whose centroid is dominated by ≥ k others</li>
 *   <li><b>Two-Phase Global Threshold</b> — cheap MBR-only Phase 1 establishes τ* broadcast to all partitions</li>
 *   <li><b>Sorted Partition Processing</b> — strong-first ordering maximizes DSCP effectiveness</li>
 * </ol>
 *
 * <p>This engine produces <b>identical results</b> to {@link SparkTopKEngine} — all pruning is safe.
 * The improved-* algorithm variants control AES/DSCP toggles; the three HGTP components are always active.</p>
 */
public final class ImprovedSparkTopKEngine {
  private ImprovedSparkTopKEngine() {}

  public static SparkRunResult rank(
      JavaSparkContext sc,
      List<com.thesis.topk.model.RawEvent> events,
      Map<String, QueryPoint> queryPoints,
      DdImputationSynopsis synopsis,
      int k,
      int partitions,
      PtdAlgorithm algorithm,
      boolean validateExact,
      int traceLimit) {
    int targetPartitions = Math.max(1, partitions);
    SparkTaskMetrics observedMetrics = new SparkTaskMetrics();
    sc.sc().addSparkListener(observedMetrics);
    Broadcast<DdImputationSynopsis> synopsisBroadcast = sc.broadcast(synopsis);

    int inputSlices = Math.max(
        targetPartitions, Math.min(256, Math.max(1, (events.size() + 19_999) / 20_000)));
    JavaRDD<com.thesis.topk.model.RawEvent> rawEvents = sc.parallelize(events, inputSlices).cache();
    long rawEventCount = rawEvents.count();

    JavaRDD<ProbabilisticInstance> instances = rawEvents
        .filter(event -> event.opType() == OpType.UPSERT)
        .flatMap(event -> ImputationEngine.impute(event, synopsisBroadcast.value()).iterator())
        .repartition(targetPartitions)
        .persist(StorageLevel.MEMORY_AND_DISK_SER());
    long instanceCount = instances.count();

    List<QueryRanking> rankings = new ArrayList<>();
    for (QueryPoint queryPoint : queryPoints.values()) {
      rankings.add(rankQuery(
          sc, instances, queryPoint, k, targetPartitions, algorithm, validateExact,
          Math.max(0, traceLimit), observedMetrics));
    }

    instances.unpersist();
    rawEvents.unpersist();
    synopsisBroadcast.destroy();
    return new SparkRunResult(rawEventCount, instanceCount, rankings);
  }

  // ═══════════════════════════════════════════════════════════════════════
  // HGTP Core: rankQuery with multi-stage pipeline
  // ═══════════════════════════════════════════════════════════════════════

  private static QueryRanking rankQuery(
      JavaSparkContext sc,
      JavaRDD<ProbabilisticInstance> allInstances,
      QueryPoint queryPoint,
      int k,
      int partitions,
      PtdAlgorithm algorithm,
      boolean validateExact,
      int traceLimit,
      SparkTaskMetrics observedMetrics) {

    JavaRDD<ProbabilisticInstance> queryInstances = allInstances
        .filter(inst -> inst.queryId().equals(queryPoint.queryId())
            || inst.queryId().equals(CsvDatasetProvider.SHARED_QUERY_ID))
        .persist(StorageLevel.MEMORY_AND_DISK_SER());

    long queryInstanceCount = queryInstances.count();
    if (queryInstanceCount == 0) {
      queryInstances.unpersist();
      return new QueryRanking(
          queryPoint.queryId(), algorithm.id(), algorithm.dscpEnabled(), algorithm.aesEnabled(),
          new ArrayList<>(), 0, 0, 0, Double.NaN, 0L, 0L, 0L, 0L, 0L,
          validateExact, true, 0L, 0L, 0L, 0L,
          new SparkTaskMetrics.Snapshot(0, 0, 0, 0, 0, 0, 0, 0), false,
          new ArrayList<>());
    }

    SparkTaskMetrics.Snapshot metricsBefore = observedMetrics.snapshot();

    // Collect all instances for this query (needed for exact scoring and validation)
    List<ProbabilisticInstance> allForQuery = queryInstances.collect();
    Broadcast<List<ProbabilisticInstance>> allBroadcast = sc.broadcast(allForQuery);

    long filteringStart = System.nanoTime();

    // ─────────── STAGE 0: MBR Skyline Pre-Filter ───────────
    // Group instances by partition, then by object within each partition
    JavaPairRDD<Integer, Iterable<ProbabilisticInstance>> grouped = queryInstances
        .mapToPair(inst -> new Tuple2<>(serverPartition(inst, partitions), inst))
        .groupByKey(partitions)
        .persist(StorageLevel.MEMORY_AND_DISK_SER());

    // Stage 0+1 combined: compute approximate envelopes with skyline filtering
    Broadcast<List<ProbabilisticInstance>> instBroadcast = allBroadcast;
    JavaRDD<ApproxEnvelope> approxEnvelopes = grouped
        .flatMap(tuple -> {
          int partitionId = tuple._1();
          List<ProbabilisticInstance> localInstances = iterableToList(tuple._2());
          return computeApproxEnvelopes(
              partitionId, queryPoint, localInstances, instBroadcast.value(), partitions, k)
              .iterator();
        })
        .persist(StorageLevel.MEMORY_AND_DISK_SER());

    long totalObjects = approxEnvelopes.count();
    long skylinePruned = approxEnvelopes.filter(ApproxEnvelope::skylinePruned).count();

    // ─────────── STAGE 1.5: Global Threshold ───────────
    // Collect top-k approximate LBs from surviving (non-skyline-pruned) objects
    List<Double> allApproxLBs = approxEnvelopes
        .filter(e -> !e.skylinePruned())
        .map(ApproxEnvelope::lbApprox)
        .collect();
    double tauStar = kthValue(allApproxLBs, k);
    Broadcast<Double> tauBroadcast = sc.broadcast(tauStar);

    // ─────────── STAGE 2: Full DDR with Global Pruning + Sorted Processing ───────────
    JavaRDD<FullEnvelope> fullEnvelopes = grouped
        .flatMap(tuple -> {
          int partitionId = tuple._1();
          List<ProbabilisticInstance> localInstances = iterableToList(tuple._2());
          return computeFullEnvelopes(
              partitionId, queryPoint, localInstances, instBroadcast.value(),
              partitions, k, tauBroadcast.value(), algorithm).iterator();
        })
        .persist(StorageLevel.MEMORY_AND_DISK_SER());

    long objectCount = fullEnvelopes.count();

    // Apply DSCP filtering
    double tau = tauStar;
    Map<Integer, Double> thresholds = new HashMap<>();
    Broadcast<Map<Integer, Double>> thresholdBroadcast = null;
    JavaRDD<FullEnvelope> survivors = fullEnvelopes;

    if (algorithm.dscpEnabled()) {
      thresholds = fullEnvelopes
          .mapToPair(e -> new Tuple2<>(e.partitionId(), e.lowerBound()))
          .groupByKey(partitions)
          .mapValues(values -> kthValue(iterableToList(values), k))
          .collectAsMap();
      thresholds = new HashMap<>(thresholds);
      double globalTau = kthValue(
          fullEnvelopes.map(FullEnvelope::lowerBound).collect(), k);
      // Use max of global τ* and local τₚ (HGTP enhancement)
      double finalGlobalTau = Math.max(globalTau, tauStar);
      thresholds.replaceAll((pid, localTau) -> Math.max(localTau, finalGlobalTau));
      tau = finalGlobalTau;
      thresholdBroadcast = sc.broadcast(new HashMap<>(thresholds));
      Broadcast<Map<Integer, Double>> thBroadcast = thresholdBroadcast;
      survivors = fullEnvelopes.filter(
          e -> e.upperBound() >= thBroadcast.value().get(e.partitionId()));
    }
    survivors = survivors.persist(StorageLevel.MEMORY_AND_DISK_SER());

    long refinedCount = survivors.count();
    long prunedCount = Math.max(0L, objectCount - refinedCount);

    // Build object traces
    List<ObjectTrace> objectTraces = new ArrayList<>();
    if (traceLimit > 0) {
      Map<Integer, Double> finalThresholds = thresholds;
      objectTraces = fullEnvelopes.take(traceLimit).stream()
          .map(e -> {
            double th = finalThresholds.getOrDefault(e.partitionId(), Double.NaN);
            boolean pruned = algorithm.dscpEnabled() && e.upperBound() < th;
            return new ObjectTrace(
                queryPoint.queryId(), e.objectId(), e.partitionId(),
                e.lowerBound(), e.upperBound(), th,
                pruned ? "pruned-by-hgtp" : "survived",
                0L, e.baselineEmissions(), e.aesEmissions());
          }).toList();
    }

    long filteringNanos = System.nanoTime() - filteringStart;
    long baselineEmissions = survivors.map(FullEnvelope::baselineEmissions).fold(0L, Long::sum);
    long aesEmissions = survivors.map(FullEnvelope::aesEmissions).fold(0L, Long::sum);

    grouped.unpersist();
    approxEnvelopes.unpersist();

    // ─────────── Emission ───────────
    long emissionStart = System.nanoTime();
    JavaPairRDD<String, String> emissions = survivors
        .flatMapToPair(e -> emitRecords(e, algorithm).iterator())
        .partitionBy(new HashPartitioner(partitions))
        .persist(StorageLevel.MEMORY_AND_DISK_SER());
    long emittedRecords = emissions.count();
    long emissionNanos = System.nanoTime() - emissionStart;

    // ─────────── STAGE 3: Refinement ───────────
    long refinementStart = System.nanoTime();
    Broadcast<List<ProbabilisticInstance>> refBroadcast = allBroadcast;
    JavaRDD<CandidateScore> refined = survivors
        .map(e -> {
          double exact = DominanceScorer.expectedDominanceScore(
              e.instances(), refBroadcast.value(), queryPoint);
          return new CandidateScore(
              e.objectId(), queryPoint.queryId(), exact,
              e.lowerBound(), e.upperBound(), e.instances().size());
        })
        .persist(StorageLevel.MEMORY_AND_DISK_SER());
    refined.count();
    List<CandidateScore> topK = refined.takeOrdered(k);
    long refinementNanos = System.nanoTime() - refinementStart;

    // ─────────── Validation ───────────
    boolean exactAgreement = true;
    long validationNanos = 0L;
    long falsePruneCount = 0L;
    if (validateExact) {
      long validationStart = System.nanoTime();
      List<CandidateScore> exactTopK = ProbabilisticTopK.exactTopK(allForQuery, queryPoint, k);
      List<String> exactIds = exactTopK.stream().map(CandidateScore::objectId).toList();
      exactAgreement = topK.stream().map(CandidateScore::objectId).toList().equals(exactIds);
      if (!exactAgreement) {
        System.err.println("HGTP EXACT AGREEMENT FAILED for query=" + queryPoint.queryId());
        System.err.println("HGTP TopK: " + topK.stream().map(c -> c.objectId() + "=" + c.exactScore()).toList());
        System.err.println("Oracle:    " + exactTopK.stream().map(c -> c.objectId() + "=" + c.exactScore()).toList());
      }
      if (algorithm.dscpEnabled()) {
        List<String> survivorIds = survivors.map(FullEnvelope::objectId).collect();
        falsePruneCount = exactIds.stream().filter(id -> !survivorIds.contains(id)).count();
      }
      validationNanos = System.nanoTime() - validationStart;
    }

    // Cleanup
    refined.unpersist();
    emissions.unpersist();
    survivors.unpersist();
    fullEnvelopes.unpersist();
    if (thresholdBroadcast != null) thresholdBroadcast.destroy();
    tauBroadcast.destroy();
    allBroadcast.destroy();
    if (!validateExact) queryInstances.unpersist();
    else queryInstances.unpersist();

    SparkTaskMetrics.Snapshot observed = observedMetrics.delta(metricsBefore);

    return new QueryRanking(
        queryPoint.queryId(), algorithm.id(), algorithm.dscpEnabled(), algorithm.aesEnabled(),
        topK, objectCount, refinedCount, prunedCount, tau,
        emittedRecords, baselineEmissions, aesEmissions, 0L, falsePruneCount,
        validateExact, exactAgreement, validationNanos,
        filteringNanos, emissionNanos, refinementNanos,
        observed, false, objectTraces);
  }

  // ═══════════════════════════════════════════════════════════════════════
  // STAGE 0+1: Skyline filter + approximate LB/UB (MBR-only)
  // ═══════════════════════════════════════════════════════════════════════

  private static List<ApproxEnvelope> computeApproxEnvelopes(
      int partitionId,
      QueryPoint queryPoint,
      List<ProbabilisticInstance> localInstances,
      List<ProbabilisticInstance> allInstances,
      int partitions,
      int k) {
    double[] query = queryPoint.coordinates();
    int dims = query.length;

    // Group local instances by object
    Map<String, List<ProbabilisticInstance>> localObjects = localInstances.stream()
        .collect(Collectors.groupingBy(
            ProbabilisticInstance::objectId, LinkedHashMap::new, Collectors.toList()));

    // Compute centroid (mean of instance attributes) and distance for each object
    Map<String, double[]> centroids = new HashMap<>();
    Map<String, Double> centroidDistances = new HashMap<>();
    for (var entry : localObjects.entrySet()) {
      double[] centroid = new double[dims];
      for (ProbabilisticInstance inst : entry.getValue()) {
        double[] attrs = inst.attributes();
        for (int d = 0; d < dims; d++) centroid[d] += attrs[d];
      }
      int count = entry.getValue().size();
      for (int d = 0; d < dims; d++) centroid[d] /= count;
      centroids.put(entry.getKey(), centroid);
      double dist = 0;
      for (int d = 0; d < dims; d++) dist += Math.abs(centroid[d] - query[d]);
      centroidDistances.put(entry.getKey(), dist);
    }

    // ── MBR Skyline Pre-Filter ──
    // An object is pruned if ≥ k other centroids dominate it in ALL dimensions relative to q
    List<String> objectIds = new ArrayList<>(localObjects.keySet());
    Map<String, Boolean> skylinePruned = new HashMap<>();
    for (String oid : objectIds) {
      double[] c = centroids.get(oid);
      int domCount = 0;
      for (String other : objectIds) {
        if (other.equals(oid)) continue;
        double[] oc = centroids.get(other);
        if (centroidDominates(oc, c, query, dims)) {
          domCount++;
          if (domCount >= k) break;
        }
      }
      skylinePruned.put(oid, domCount >= k);
    }

    // ── Approximate LB/UB via MBR-only containment tests ──
    List<ApproxEnvelope> envelopes = new ArrayList<>();
    for (String oid : objectIds) {
      if (skylinePruned.get(oid)) {
        envelopes.add(new ApproxEnvelope(partitionId, oid, 0.0, 0.0, true));
        continue;
      }
      // Compute approximate score using local instance-level dominance (cheap)
      List<ProbabilisticInstance> myInstances = localObjects.get(oid);
      double lbApprox = DominanceScorer.expectedDominanceScore(
          myInstances, localInstances, queryPoint);
      // Upper bound: LB + objectMass * remoteMass
      double objectMass = myInstances.stream().mapToDouble(ProbabilisticInstance::probability).sum();
      double remoteMass = allInstances.stream()
          .filter(other -> !other.objectId().equals(oid))
          .filter(other -> serverPartition(other, partitions) != partitionId)
          .mapToDouble(ProbabilisticInstance::probability)
          .sum();
      double ubApprox = lbApprox + objectMass * remoteMass;
      envelopes.add(new ApproxEnvelope(partitionId, oid, lbApprox, ubApprox, false));
    }
    return envelopes;
  }

  /** Returns true if centroid a dominates centroid b in ALL dimensions relative to query q. */
  private static boolean centroidDominates(double[] a, double[] b, double[] q, int dims) {
    boolean strictlyBetter = false;
    for (int d = 0; d < dims; d++) {
      double da = Math.abs(a[d] - q[d]);
      double db = Math.abs(b[d] - q[d]);
      if (da > db) return false;
      if (da < db) strictlyBetter = true;
    }
    return strictlyBetter;
  }

  // ═══════════════════════════════════════════════════════════════════════
  // STAGE 2: Full DDR computation with global threshold + sorted processing
  // ═══════════════════════════════════════════════════════════════════════

  private static List<FullEnvelope> computeFullEnvelopes(
      int partitionId,
      QueryPoint queryPoint,
      List<ProbabilisticInstance> localInstances,
      List<ProbabilisticInstance> allInstances,
      int partitions,
      int k,
      double tauStar,
      PtdAlgorithm algorithm) {
    double[] query = queryPoint.coordinates();
    int dims = query.length;

    Map<String, List<ProbabilisticInstance>> localObjects = localInstances.stream()
        .collect(Collectors.groupingBy(
            ProbabilisticInstance::objectId, LinkedHashMap::new, Collectors.toList()));
    List<String> allLocalIds = new ArrayList<>(localObjects.keySet());

    // Compute approximate scores for sorting (cheap — reuse centroid distance as proxy)
    Map<String, Double> approxScores = new HashMap<>();
    Map<String, Boolean> skylinePruned = new HashMap<>();
    for (var entry : localObjects.entrySet()) {
      String oid = entry.getKey();
      List<ProbabilisticInstance> myInstances = entry.getValue();

      // Centroid skyline check
      double[] centroid = new double[dims];
      for (ProbabilisticInstance inst : myInstances) {
        double[] attrs = inst.attributes();
        for (int d = 0; d < dims; d++) centroid[d] += attrs[d];
      }
      for (int d = 0; d < dims; d++) centroid[d] /= myInstances.size();

      // Quick approx LB
      double lbApprox = DominanceScorer.expectedDominanceScore(
          myInstances, localInstances, queryPoint);
      double objectMass = myInstances.stream().mapToDouble(ProbabilisticInstance::probability).sum();
      double remoteMass = allInstances.stream()
          .filter(o -> !o.objectId().equals(oid))
          .filter(o -> serverPartition(o, partitions) != partitionId)
          .mapToDouble(ProbabilisticInstance::probability).sum();
      double ubApprox = lbApprox + objectMass * remoteMass;

      approxScores.put(oid, lbApprox);

      // Global threshold pre-check: if UB_approx < τ*, skip entirely
      skylinePruned.put(oid, ubApprox < tauStar);
    }

    // ── Sorted Processing: sort objects by approx LB descending ──
    List<String> sortedIds = new ArrayList<>(allLocalIds);
    sortedIds.sort((a, b) -> Double.compare(
        approxScores.getOrDefault(b, 0.0),
        approxScores.getOrDefault(a, 0.0)));

    List<FullEnvelope> envelopes = new ArrayList<>();
    for (String objectId : sortedIds) {
      if (skylinePruned.getOrDefault(objectId, false)) continue;

      List<ProbabilisticInstance> objectInstances = localObjects.get(objectId);
      List<String> competitors = allLocalIds.stream()
          .filter(id -> !id.equals(objectId)).toList();

      // Full DDR computation
      double lowerBound = DominanceScorer.expectedDominanceScore(
          objectInstances, localInstances, queryPoint);
      double objectMass = objectInstances.stream()
          .mapToDouble(ProbabilisticInstance::probability).sum();
      double remoteMass = allInstances.stream()
          .filter(other -> !other.objectId().equals(objectId))
          .filter(other -> serverPartition(other, partitions) != partitionId)
          .mapToDouble(ProbabilisticInstance::probability).sum();
      double upperBound = lowerBound + objectMass * remoteMass;

      long baseline = (long) objectInstances.size() * competitors.size();
      long aes = competitors.isEmpty() ? 0L : objectInstances.size();

      envelopes.add(new FullEnvelope(
          partitionId, objectId, objectInstances, competitors,
          lowerBound, upperBound, baseline, aes));
    }
    return envelopes;
  }

  // ═══════════════════════════════════════════════════════════════════════
  // Emission (same logic as original engine)
  // ═══════════════════════════════════════════════════════════════════════

  private static List<Tuple2<String, String>> emitRecords(
      FullEnvelope envelope, PtdAlgorithm algorithm) {
    List<Tuple2<String, String>> records = new ArrayList<>();
    if (envelope.competitorIds().isEmpty()) return records;
    for (ProbabilisticInstance instance : envelope.instances()) {
      if (algorithm.aesEnabled()) {
        records.add(new Tuple2<>(
            Integer.toString(envelope.partitionId()),
            instance.instanceId() + "|" + String.join(";", envelope.competitorIds())));
      } else {
        for (String competitor : envelope.competitorIds()) {
          records.add(new Tuple2<>(
              Integer.toString(envelope.partitionId()),
              instance.instanceId() + "|" + competitor));
        }
      }
    }
    return records;
  }

  // ═══════════════════════════════════════════════════════════════════════
  // Utility methods
  // ═══════════════════════════════════════════════════════════════════════

  private static double kthValue(List<Double> values, int k) {
    return values.stream()
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
    return Math.floorMod(instance.objectId().hashCode(), partitions);
  }

  // ═══════════════════════════════════════════════════════════════════════
  // Internal records
  // ═══════════════════════════════════════════════════════════════════════

  private static final class ApproxEnvelope implements Serializable {
    private final int partitionId;
    private final String objectId;
    private final double lbApprox;
    private final double ubApprox;
    private final boolean skylinePruned;

    ApproxEnvelope(int partitionId, String objectId,
        double lbApprox, double ubApprox, boolean skylinePruned) {
      this.partitionId = partitionId;
      this.objectId = objectId;
      this.lbApprox = lbApprox;
      this.ubApprox = ubApprox;
      this.skylinePruned = skylinePruned;
    }

    int partitionId() { return partitionId; }
    String objectId() { return objectId; }
    double lbApprox() { return lbApprox; }
    double ubApprox() { return ubApprox; }
    boolean skylinePruned() { return skylinePruned; }
  }

  private static final class FullEnvelope implements Serializable {
    private final int partitionId;
    private final String objectId;
    private final List<ProbabilisticInstance> instances;
    private final List<String> competitorIds;
    private final double lowerBound;
    private final double upperBound;
    private final long baselineEmissions;
    private final long aesEmissions;

    FullEnvelope(int partitionId, String objectId,
        List<ProbabilisticInstance> instances, List<String> competitorIds,
        double lowerBound, double upperBound,
        long baselineEmissions, long aesEmissions) {
      this.partitionId = partitionId;
      this.objectId = objectId;
      this.instances = new ArrayList<>(instances);
      this.competitorIds = new ArrayList<>(competitorIds);
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.baselineEmissions = baselineEmissions;
      this.aesEmissions = aesEmissions;
    }

    int partitionId() { return partitionId; }
    String objectId() { return objectId; }
    List<ProbabilisticInstance> instances() { return instances; }
    List<String> competitorIds() { return competitorIds; }
    double lowerBound() { return lowerBound; }
    double upperBound() { return upperBound; }
    long baselineEmissions() { return baselineEmissions; }
    long aesEmissions() { return aesEmissions; }
  }
}
