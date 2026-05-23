package com.thesis.topk.dataset;

import com.thesis.topk.model.ImputationRule;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class AllRawDatasetProvider implements DatasetProvider {
  @Override
  public String name() {
    return "all";
  }

  @Override
  public SimulationData generate(SimulationConfig config, Args args) {
    List<RawEvent> events = new ArrayList<>();
    Map<String, QueryPoint> queries = new LinkedHashMap<>();
    int dimensions = 0;
    for (DatasetProvider provider : DatasetProviders.allRawDatasets()) {
      SimulationData data = provider.generate(config, args);
      events.addAll(data.events());
      queries.putAll(data.queryPoints());
      for (ImputationRule rule : data.rules().values()) {
        dimensions = Math.max(dimensions, rule.dimension() + 1);
      }
    }
    return new SimulationData(List.copyOf(events), DatasetDefaults.rules(dimensions), queries);
  }
}
