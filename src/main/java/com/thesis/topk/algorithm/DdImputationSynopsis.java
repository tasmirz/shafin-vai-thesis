package com.thesis.topk.algorithm;

import com.thesis.topk.model.RawEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compact conditional histograms used as a DD-style imputation synopsis.
 *
 * <p>For each query family and missing target dimension, training selects the determinant
 * dimension whose equi-width buckets minimize expected matching candidates while retaining
 * observed support. Bucket value summaries are emitted as probabilistic repairs.</p>
 */
public final class DdImputationSynopsis implements Serializable {
  private final Map<String, Map<Integer, ConditionalRule>> rulesByQuery;
  private final int bins;

  private DdImputationSynopsis(Map<String, Map<Integer, ConditionalRule>> rulesByQuery, int bins) {
    this.rulesByQuery = rulesByQuery;
    this.bins = bins;
  }

  public static DdImputationSynopsis train(List<RawEvent> events, int bins) {
    Map<String, List<RawEvent>> grouped = new LinkedHashMap<>();
    for (RawEvent event : events) {
      grouped.computeIfAbsent(event.queryId(), ignored -> new ArrayList<>()).add(event);
    }
    Map<String, Map<Integer, ConditionalRule>> rules = new LinkedHashMap<>();
    for (Map.Entry<String, List<RawEvent>> entry : grouped.entrySet()) {
      rules.put(entry.getKey(), trainRules(entry.getValue(), Math.max(2, bins)));
    }
    return new DdImputationSynopsis(rules, Math.max(2, bins));
  }

  public List<ValueCandidate> candidates(RawEvent event, int dimension) {
    Map<Integer, ConditionalRule> rules = rulesByQuery.get(event.queryId());
    if (rules == null || !rules.containsKey(dimension)) {
      return new ArrayList<>();
    }
    return rules.get(dimension).candidates(event);
  }

  public int ruleCount() {
    return rulesByQuery.values().stream().mapToInt(Map::size).sum();
  }

  public int bins() {
    return bins;
  }

  public double averageEstimatedCandidateCount() {
    return rulesByQuery.values().stream()
        .flatMap(map -> map.values().stream())
        .mapToDouble(ConditionalRule::estimatedCandidateCount)
        .average()
        .orElse(0.0);
  }

  public Evaluation evaluate(List<RawEvent> holdout) {
    double totalAbsoluteError = 0.0;
    int evaluated = 0;
    for (RawEvent event : holdout) {
      double[] attributes = event.attributes();
      boolean[] missing = event.missingMask();
      for (int dimension = 0; dimension < attributes.length; dimension++) {
        if (missing[dimension] || Double.isNaN(attributes[dimension])) {
          continue;
        }
        double[] maskedAttributes = attributes.clone();
        boolean[] masked = missing.clone();
        maskedAttributes[dimension] = Double.NaN;
        masked[dimension] = true;
        RawEvent maskedEvent = new RawEvent(
            event.objectId(), event.queryId(), event.eventTime(), maskedAttributes, masked, event.opType());
        List<ValueCandidate> candidates = candidates(maskedEvent, dimension);
        if (candidates.isEmpty()) {
          continue;
        }
        double expected = candidates.stream()
            .mapToDouble(candidate -> candidate.value() * candidate.probability())
            .sum();
        totalAbsoluteError += Math.abs(attributes[dimension] - expected);
        evaluated++;
      }
    }
    return new Evaluation(evaluated, evaluated == 0 ? Double.NaN : totalAbsoluteError / evaluated);
  }

  private static Map<Integer, ConditionalRule> trainRules(List<RawEvent> events, int bins) {
    Map<Integer, ConditionalRule> rules = new LinkedHashMap<>();
    int dimensions = events.stream().mapToInt(event -> event.attributes().length).max().orElse(0);
    for (int target = 0; target < dimensions; target++) {
      List<Double> targetValues = observedValues(events, target);
      if (targetValues.isEmpty()) {
        continue;
      }
      List<ValueCandidate> fallback = summarize(targetValues);
      ConditionalRule best = new ConditionalRule(target, -1, 0.0, 1.0, new ArrayList<>(), fallback,
          targetValues.size());
      for (int determinant = 0; determinant < dimensions; determinant++) {
        if (determinant == target) {
          continue;
        }
        List<Pair> support = observedPairs(events, determinant, target);
        if (support.size() < 2) {
          continue;
        }
        ConditionalRule candidate = fromPairs(target, determinant, support, fallback, bins);
        if (candidate.estimatedCandidateCount() < best.estimatedCandidateCount()) {
          best = candidate;
        }
      }
      rules.put(target, best);
    }
    return rules;
  }

  private static ConditionalRule fromPairs(
      int target,
      int determinant,
      List<Pair> support,
      List<ValueCandidate> fallback,
      int bins) {
    double min = support.stream().mapToDouble(Pair::determinant).min().orElse(0.0);
    double max = support.stream().mapToDouble(Pair::determinant).max().orElse(min);
    List<List<Double>> values = new ArrayList<>(bins);
    for (int i = 0; i < bins; i++) {
      values.add(new ArrayList<>());
    }
    for (Pair pair : support) {
      values.get(bucket(pair.determinant(), min, max, bins)).add(pair.target());
    }
    List<List<ValueCandidate>> summaries = new ArrayList<>(bins);
    int occupied = 0;
    for (List<Double> bucket : values) {
      summaries.add(summarize(bucket));
      if (!bucket.isEmpty()) {
        occupied++;
      }
    }
    double estimatedCandidates = occupied == 0
        ? fallback.size()
        : (double) support.size() / occupied;
    return new ConditionalRule(
        target, determinant, min, max, summaries, fallback, estimatedCandidates);
  }

  private static List<Double> observedValues(List<RawEvent> events, int dimension) {
    List<Double> values = new ArrayList<>();
    for (RawEvent event : events) {
      double[] attributes = event.attributes();
      boolean[] missing = event.missingMask();
      if (dimension < attributes.length && !missing[dimension] && !Double.isNaN(attributes[dimension])) {
        values.add(attributes[dimension]);
      }
    }
    return values;
  }

  private static List<Pair> observedPairs(List<RawEvent> events, int determinant, int target) {
    List<Pair> pairs = new ArrayList<>();
    for (RawEvent event : events) {
      double[] attributes = event.attributes();
      boolean[] missing = event.missingMask();
      if (determinant < attributes.length && target < attributes.length
          && !missing[determinant] && !missing[target]
          && !Double.isNaN(attributes[determinant]) && !Double.isNaN(attributes[target])) {
        pairs.add(new Pair(attributes[determinant], attributes[target]));
      }
    }
    return pairs;
  }

  private static List<ValueCandidate> summarize(List<Double> source) {
    List<ValueCandidate> result = new ArrayList<>();
    if (source.isEmpty()) {
      return result;
    }
    List<Double> values = new ArrayList<>(source);
    values.sort(Comparator.naturalOrder());
    if (values.size() == 1) {
      result.add(new ValueCandidate(values.get(0), 1.0));
      return result;
    }
    int midpoint = Math.max(1, values.size() / 2);
    List<Double> low = values.subList(0, midpoint);
    List<Double> high = values.subList(midpoint, values.size());
    if (high.isEmpty()) {
      result.add(new ValueCandidate(mean(low), 1.0));
      return result;
    }
    result.add(new ValueCandidate(mean(low), (double) low.size() / values.size()));
    result.add(new ValueCandidate(mean(high), (double) high.size() / values.size()));
    return result;
  }

  private static double mean(List<Double> values) {
    return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
  }

  private static int bucket(double value, double min, double max, int bins) {
    if (max <= min) {
      return 0;
    }
    double scaled = (value - min) / (max - min);
    return Math.min(bins - 1, Math.max(0, (int) Math.floor(scaled * bins)));
  }

  private static final class Pair implements Serializable {
    private final double determinant;
    private final double target;

    Pair(double determinant, double target) {
      this.determinant = determinant;
      this.target = target;
    }

    double determinant() {
      return determinant;
    }

    double target() {
      return target;
    }
  }

  private static final class ConditionalRule implements Serializable {
    private final int targetDimension;
    private final int determinantDimension;
    private final double determinantMin;
    private final double determinantMax;
    private final List<List<ValueCandidate>> buckets;
    private final List<ValueCandidate> fallback;
    private final double estimatedCandidateCount;

    ConditionalRule(
        int targetDimension,
        int determinantDimension,
        double determinantMin,
        double determinantMax,
        List<List<ValueCandidate>> buckets,
        List<ValueCandidate> fallback,
        double estimatedCandidateCount) {
      this.targetDimension = targetDimension;
      this.determinantDimension = determinantDimension;
      this.determinantMin = determinantMin;
      this.determinantMax = determinantMax;
      this.buckets = buckets;
      this.fallback = fallback;
      this.estimatedCandidateCount = estimatedCandidateCount;
    }

    int targetDimension() { return targetDimension; }
    int determinantDimension() { return determinantDimension; }
    double determinantMin() { return determinantMin; }
    double determinantMax() { return determinantMax; }
    List<List<ValueCandidate>> buckets() { return buckets; }
    List<ValueCandidate> fallback() { return fallback; }
    double estimatedCandidateCount() { return estimatedCandidateCount; }

    List<ValueCandidate> candidates(RawEvent event) {
      if (determinantDimension < 0 || determinantDimension >= event.attributes().length) {
        return fallback;
      }
      boolean[] missing = event.missingMask();
      double[] attributes = event.attributes();
      if (missing[determinantDimension] || Double.isNaN(attributes[determinantDimension])) {
        return fallback;
      }
      List<ValueCandidate> match = buckets.get(
          bucket(attributes[determinantDimension], determinantMin, determinantMax, buckets.size()));
      return match.isEmpty() ? fallback : match;
    }
  }

  public static final class ValueCandidate implements Serializable {
    private final double value;
    private final double probability;

    public ValueCandidate(double value, double probability) {
      this.value = value;
      this.probability = probability;
    }

    public double value() {
      return value;
    }

    public double probability() {
      return probability;
    }
  }

  public static final class Evaluation implements Serializable {
    private final int evaluatedValues;
    private final double meanAbsoluteError;

    public Evaluation(int evaluatedValues, double meanAbsoluteError) {
      this.evaluatedValues = evaluatedValues;
      this.meanAbsoluteError = meanAbsoluteError;
    }

    public int evaluatedValues() {
      return evaluatedValues;
    }

    public double meanAbsoluteError() {
      return meanAbsoluteError;
    }
  }
}
