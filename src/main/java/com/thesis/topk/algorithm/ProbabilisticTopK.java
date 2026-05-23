package com.thesis.topk.algorithm;

import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ProbabilisticTopK {
  private ProbabilisticTopK() {
  }

  public static List<CandidateScore> exactTopK(
      Collection<ProbabilisticInstance> instances,
      QueryPoint queryPoint,
      int k) {
    return exactTopKForObjects(instances, instances, queryPoint, k);
  }

  public static List<CandidateScore> exactTopKForObjects(
      Collection<ProbabilisticInstance> candidateInstances,
      Collection<ProbabilisticInstance> allInstances,
      QueryPoint queryPoint,
      int k) {
    Map<String, List<ProbabilisticInstance>> grouped = groupByObject(candidateInstances);
    List<ProbabilisticInstance> all = List.copyOf(allInstances);
    List<CandidateScore> scores = new ArrayList<>();
    for (Map.Entry<String, List<ProbabilisticInstance>> entry : grouped.entrySet()) {
      double exact = DominanceScorer.expectedDominanceScore(entry.getValue(), all, queryPoint);
      scores.add(new CandidateScore(entry.getKey(), queryPoint.queryId(), exact, exact, exact, entry.getValue().size()));
    }
    return topK(scores, k);
  }

  public static PrunedRanking prunedTopK(
      Collection<ProbabilisticInstance> instances,
      QueryPoint queryPoint,
      int k) {
    Map<String, List<ProbabilisticInstance>> grouped = groupByObject(instances);
    List<ProbabilisticInstance> all = List.copyOf(instances);
    List<CandidateScore> roughScores = new ArrayList<>();

    for (Map.Entry<String, List<ProbabilisticInstance>> entry : grouped.entrySet()) {
      double lb = lowerBound(entry.getValue(), all, queryPoint);
      double ub = DominanceScorer.closenessUpperBound(entry.getValue(), all, queryPoint);
      roughScores.add(new CandidateScore(entry.getKey(), queryPoint.queryId(), lb, lb, ub, entry.getValue().size()));
    }

    double threshold = kthLowerBound(roughScores, k);
    List<CandidateScore> refined = new ArrayList<>();
    int pruned = 0;
    for (CandidateScore rough : roughScores) {
      if (rough.upperBound() < threshold) {
        pruned++;
        continue;
      }
      List<ProbabilisticInstance> objectInstances = grouped.get(rough.objectId());
      double exact = DominanceScorer.expectedDominanceScore(objectInstances, all, queryPoint);
      refined.add(new CandidateScore(
          rough.objectId(),
          rough.queryId(),
          exact,
          rough.lowerBound(),
          rough.upperBound(),
          rough.instanceCount()));
    }
    return new PrunedRanking(topK(refined, k), grouped.size(), refined.size(), pruned);
  }

  public static PrunedRanking candidatePrunedTopK(
      Collection<ProbabilisticInstance> instances,
      QueryPoint queryPoint,
      int k,
      int candidateLimit) {
    Map<String, List<ProbabilisticInstance>> grouped = groupByObject(instances);
    List<ProbabilisticInstance> all = List.copyOf(instances);
    int limit = Math.max(k, Math.min(candidateLimit, grouped.size()));
    List<CandidateScore> candidateProxies = new ArrayList<>();

    for (Map.Entry<String, List<ProbabilisticInstance>> entry : grouped.entrySet()) {
      double proxy = dominanceProxy(entry.getValue(), queryPoint);
      candidateProxies.add(new CandidateScore(
          entry.getKey(),
          queryPoint.queryId(),
          proxy,
          proxy,
          proxy,
          entry.getValue().size()));
    }

    List<CandidateScore> candidates = topK(candidateProxies, limit);
    List<CandidateScore> refined = new ArrayList<>(candidates.size());
    for (CandidateScore candidate : candidates) {
      List<ProbabilisticInstance> objectInstances = grouped.get(candidate.objectId());
      double exact = DominanceScorer.expectedDominanceScore(objectInstances, all, queryPoint);
      refined.add(new CandidateScore(
          candidate.objectId(),
          candidate.queryId(),
          exact,
          candidate.lowerBound(),
          candidate.upperBound(),
          candidate.instanceCount()));
    }
    return new PrunedRanking(topK(refined, k), grouped.size(), refined.size(), grouped.size() - refined.size());
  }

  public static List<CandidateScore> topK(List<CandidateScore> scores, int k) {
    return scores.stream()
        .sorted()
        .limit(k)
        .toList();
  }

  private static double lowerBound(
      List<ProbabilisticInstance> objectInstances,
      List<ProbabilisticInstance> allInstances,
      QueryPoint queryPoint) {
    double exact = DominanceScorer.expectedDominanceScore(objectInstances, allInstances, queryPoint);
    return Math.max(0.0, exact - bestProbability(objectInstances));
  }

  private static double bestProbability(List<ProbabilisticInstance> instances) {
    return instances.stream().mapToDouble(ProbabilisticInstance::probability).max().orElse(0.0);
  }

  private static double dominanceProxy(List<ProbabilisticInstance> instances, QueryPoint queryPoint) {
    double[] q = queryPoint.coordinates();
    double score = 0.0;
    for (ProbabilisticInstance instance : instances) {
      double[] attributes = instance.attributes();
      double distance = 0.0;
      for (int d = 0; d < q.length; d++) {
        distance += Math.abs(attributes[d] - q[d]);
      }
      score += instance.probability() * (q.length - distance);
    }
    return score;
  }

  private static double kthLowerBound(List<CandidateScore> roughScores, int k) {
    return roughScores.stream()
        .map(CandidateScore::lowerBound)
        .sorted(Comparator.reverseOrder())
        .skip(Math.max(0, k - 1L))
        .findFirst()
        .orElse(Double.NEGATIVE_INFINITY);
  }

  private static Map<String, List<ProbabilisticInstance>> groupByObject(
      Collection<ProbabilisticInstance> instances) {
    Map<String, List<ProbabilisticInstance>> grouped = new LinkedHashMap<>();
    for (ProbabilisticInstance instance : instances) {
      grouped.computeIfAbsent(instance.objectId(), ignored -> new ArrayList<>()).add(instance);
    }
    return grouped;
  }

  public record PrunedRanking(
      List<CandidateScore> topK,
      int objectCount,
      int refinedCount,
      int prunedCount) {
    public double pruneRatio() {
      return objectCount == 0 ? 0.0 : (double) prunedCount / objectCount;
    }
  }
}
