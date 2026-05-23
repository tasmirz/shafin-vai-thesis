package com.thesis.topk.algorithm;

import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import java.util.List;

public final class DominanceScorer {
  private DominanceScorer() {
  }

  public static boolean dynamicallyDominates(
      ProbabilisticInstance a,
      ProbabilisticInstance b,
      QueryPoint queryPoint) {
    double[] qa = queryPoint.coordinates();
    double[] aa = a.attributes();
    double[] bb = b.attributes();
    boolean strictlyBetter = false;

    for (int d = 0; d < qa.length; d++) {
      double da = Math.abs(aa[d] - qa[d]);
      double db = Math.abs(bb[d] - qa[d]);
      if (da > db) {
        return false;
      }
      if (da < db) {
        strictlyBetter = true;
      }
    }
    return strictlyBetter;
  }

  public static double expectedDominanceScore(
      List<ProbabilisticInstance> objectInstances,
      List<ProbabilisticInstance> allInstances,
      QueryPoint queryPoint) {
    double score = 0.0;
    for (ProbabilisticInstance mine : objectInstances) {
      double dominatedMass = 0.0;
      for (ProbabilisticInstance other : allInstances) {
        if (!mine.objectId().equals(other.objectId())
            && dynamicallyDominates(mine, other, queryPoint)) {
          dominatedMass += other.probability();
        }
      }
      score += mine.probability() * dominatedMass;
    }
    return score;
  }

  public static double closenessUpperBound(
      List<ProbabilisticInstance> objectInstances,
      List<ProbabilisticInstance> allInstances,
      QueryPoint queryPoint) {
    double exact = expectedDominanceScore(objectInstances, allInstances, queryPoint);
    double maxRemainingMass = allInstances.stream()
        .filter(i -> !i.objectId().equals(objectInstances.get(0).objectId()))
        .mapToDouble(ProbabilisticInstance::probability)
        .sum();
    double bestInstanceProbability = objectInstances.stream()
        .mapToDouble(ProbabilisticInstance::probability)
        .max()
        .orElse(0.0);
    return Math.min(maxRemainingMass, exact + bestInstanceProbability);
  }
}

