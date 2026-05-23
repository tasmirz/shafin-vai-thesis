package com.thesis.topk.dataset;

import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class SyntheticDatasetProvider implements DatasetProvider {
  @Override
  public String name() {
    return "synthetic";
  }

  @Override
  public SimulationData generate(SimulationConfig config, Args args) {
    Random random = new Random(config.seed());
    List<RawEvent> events = new ArrayList<>(config.objects() * config.queries());

    for (int q = 0; q < config.queries(); q++) {
      String queryId = "q" + q;
      for (int i = 0; i < config.objects(); i++) {
        double[] attributes = new double[config.dimensions()];
        boolean[] missing = new boolean[config.dimensions()];
        for (int d = 0; d < config.dimensions(); d++) {
          double cluster = ((i % 17) / 16.0) * 0.45;
          attributes[d] = DatasetDefaults.clamp(cluster + (0.55 * random.nextDouble()));
          missing[d] = random.nextDouble() < config.missingRate();
          if (missing[d]) {
            attributes[d] = Double.NaN;
          }
        }
        events.add(new RawEvent(
            "obj-" + i,
            queryId,
            config.startTimeMs() + (long) events.size() * config.eventGapMs(),
            attributes,
            missing,
            OpType.UPSERT));
      }
    }
    return new SimulationData(
        List.copyOf(events),
        DatasetDefaults.rules(config.dimensions()),
        DatasetDefaults.queryPoints(config.queries(), config.dimensions()));
  }
}
