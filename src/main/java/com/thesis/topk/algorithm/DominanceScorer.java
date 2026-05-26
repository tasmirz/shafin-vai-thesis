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

  /** Returns true only when the candidate instance dominates every point in the competitor MBR. */
  public static boolean dynamicallyDominatesMbrFully(
      ProbabilisticInstance candidate,
      double[] mbrMin,
      double[] mbrMax,
      QueryPoint queryPoint) {
    double[] query = queryPoint.coordinates();
    double[] attributes = candidate.attributes();
    boolean strictlyBetter = false;
    for (int d = 0; d < query.length; d++) {
      double candidateDistance = Math.abs(attributes[d] - query[d]);
      double minimumMbrDistance = minimumDistance(query[d], mbrMin[d], mbrMax[d]);
      if (candidateDistance > minimumMbrDistance) {
        return false;
      }
      if (candidateDistance < minimumMbrDistance) {
        strictlyBetter = true;
      }
    }
    return strictlyBetter;
  }

  /** Returns true when the candidate can dominate at least one point in the competitor MBR. */
  public static boolean dynamicallyDominatesMbrPossibly(
      ProbabilisticInstance candidate,
      double[] mbrMin,
      double[] mbrMax,
      QueryPoint queryPoint) {
    double[] query = queryPoint.coordinates();
    double[] attributes = candidate.attributes();
    boolean strictlyBetter = false;
    for (int d = 0; d < query.length; d++) {
      double candidateDistance = Math.abs(attributes[d] - query[d]);
      double maximumMbrDistance = Math.max(
          Math.abs(mbrMin[d] - query[d]), Math.abs(mbrMax[d] - query[d]));
      if (candidateDistance > maximumMbrDistance) {
        return false;
      }
      if (candidateDistance < maximumMbrDistance) {
        strictlyBetter = true;
      }
    }
    return strictlyBetter;
  }

  private static double minimumDistance(double query, double lower, double upper) {
    if (query >= lower && query <= upper) {
      return 0.0;
    }
    return Math.min(Math.abs(lower - query), Math.abs(upper - query));
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
