package com.thesis.topk.dataset;

import com.thesis.topk.model.ImputationRule;
import com.thesis.topk.model.QueryPoint;
import java.util.LinkedHashMap;
import java.util.Map;

final class DatasetDefaults {
  private DatasetDefaults() {
  }

  static Map<Integer, ImputationRule> rules(int dimensions) {
    Map<Integer, ImputationRule> rules = new LinkedHashMap<>();
    for (int d = 0; d < dimensions; d++) {
      double low = 0.15 + d * 0.03;
      double high = 0.75 - d * 0.02;
      double p = Math.min(0.80, 0.45 + d * 0.05);
      rules.put(d, new ImputationRule(d, low, high, p));
    }
    return rules;
  }

  static Map<String, QueryPoint> queryPoints(int queries, int dimensions) {
    Map<String, QueryPoint> points = new LinkedHashMap<>();
    for (int q = 0; q < queries; q++) {
      String queryId = "q" + q;
      points.put(queryId, new QueryPoint(queryId, queryCoordinates(q, dimensions)));
    }
    return points;
  }

  static QueryPoint queryPoint(String queryId, int dimensions) {
    int queryOffset = parseQueryOffset(queryId);
    return new QueryPoint(queryId, queryCoordinates(queryOffset, dimensions));
  }

  private static double[] queryCoordinates(int queryOffset, int dimensions) {
    double[] coords = new double[dimensions];
    for (int d = 0; d < dimensions; d++) {
      coords[d] = clamp(0.2 + queryOffset * 0.15 + d * 0.05);
    }
    return coords;
  }

  static double clamp(double value) {
    return Math.max(0.0, Math.min(1.0, value));
  }

  private static int parseQueryOffset(String queryId) {
    if (queryId != null && queryId.length() > 1 && queryId.charAt(0) == 'q') {
      try {
        return Integer.parseInt(queryId.substring(1));
      } catch (NumberFormatException ignored) {
        return 0;
      }
    }
    return 0;
  }
}
