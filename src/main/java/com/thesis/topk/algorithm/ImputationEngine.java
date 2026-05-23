package com.thesis.topk.algorithm;

import com.thesis.topk.model.ImputationRule;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.RawEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ImputationEngine {
  private ImputationEngine() {
  }

  public static List<ProbabilisticInstance> impute(
      RawEvent event,
      Map<Integer, ImputationRule> rules) {
    double[] base = event.attributes();
    boolean[] missing = event.missingMask();
    List<PartialRepair> repairs = new ArrayList<>();
    repairs.add(new PartialRepair(base, 1.0));

    for (int d = 0; d < missing.length; d++) {
      if (!missing[d]) {
        continue;
      }
      ImputationRule rule = rules.get(d);
      if (rule == null) {
        rule = new ImputationRule(d, 0.25, 0.75, 0.5);
      }
      List<PartialRepair> next = new ArrayList<>(repairs.size() * 2);
      for (PartialRepair repair : repairs) {
        double[] low = repair.attributes.clone();
        low[d] = rule.lowValue();
        next.add(new PartialRepair(low, repair.probability * rule.lowProbability()));

        double[] high = repair.attributes.clone();
        high[d] = rule.highValue();
        next.add(new PartialRepair(high, repair.probability * (1.0 - rule.lowProbability())));
      }
      repairs = next;
    }

    return instances(event, repairs);
  }

  public static List<ProbabilisticInstance> impute(RawEvent event, DdImputationSynopsis synopsis) {
    double[] base = event.attributes();
    boolean[] missing = event.missingMask();
    List<PartialRepair> repairs = new ArrayList<>();
    repairs.add(new PartialRepair(base, 1.0));

    for (int d = 0; d < missing.length; d++) {
      if (!missing[d]) {
        continue;
      }
      List<DdImputationSynopsis.ValueCandidate> candidates = synopsis.candidates(event, d);
      if (candidates.isEmpty()) {
        candidates = List.of(
            new DdImputationSynopsis.ValueCandidate(0.25, 0.5),
            new DdImputationSynopsis.ValueCandidate(0.75, 0.5));
      }
      List<PartialRepair> next = new ArrayList<>(repairs.size() * candidates.size());
      for (PartialRepair repair : repairs) {
        for (DdImputationSynopsis.ValueCandidate candidate : candidates) {
          double[] attributes = repair.attributes.clone();
          attributes[d] = candidate.value();
          next.add(new PartialRepair(attributes, repair.probability * candidate.probability()));
        }
      }
      repairs = next;
    }
    return instances(event, repairs);
  }

  private static List<ProbabilisticInstance> instances(RawEvent event, List<PartialRepair> repairs) {
    List<ProbabilisticInstance> instances = new ArrayList<>(repairs.size());
    double total = repairs.stream().mapToDouble(r -> r.probability).sum();
    for (int i = 0; i < repairs.size(); i++) {
      PartialRepair repair = repairs.get(i);
      double normalized = total == 0.0 ? 0.0 : repair.probability / total;
      instances.add(new ProbabilisticInstance(
          event.objectId(),
          event.queryId(),
          event.objectId() + "#i" + i,
          event.eventTime(),
          normalized,
          repair.attributes));
    }
    return instances;
  }

  private record PartialRepair(double[] attributes, double probability) {
  }
}
