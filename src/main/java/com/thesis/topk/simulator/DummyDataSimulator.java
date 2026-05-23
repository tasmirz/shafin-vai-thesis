package com.thesis.topk.simulator;

import com.thesis.topk.dataset.SyntheticDatasetProvider;

public final class DummyDataSimulator {
  private DummyDataSimulator() {
  }

  public static SimulationData generate(SimulationConfig config) {
    return new SyntheticDatasetProvider().generate(config, Args.parse(new String[0]));
  }
}
