package com.thesis.topk.dataset;

import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;

public interface DatasetProvider {
  String name();

  SimulationData generate(SimulationConfig config, Args args);

  default String mqttTopic() {
    return "thesis/raw/" + name();
  }

  default String kafkaTopic() {
    return "thesis.raw." + name();
  }
}
