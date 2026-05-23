package com.thesis.topk.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DummyDataSimulatorTest {
  @Test
  void generationIsDeterministicForSameSeed() {
    SimulationConfig config = new SimulationConfig(10, 3, 2, 4, 0.25, 123L, 1000L, 5L);

    SimulationData first = DummyDataSimulator.generate(config);
    SimulationData second = DummyDataSimulator.generate(config);

    assertThat(first.events().toString()).isEqualTo(second.events().toString());
    assertThat(first.queryPoints().toString()).isEqualTo(second.queryPoints().toString());
  }

  @Test
  void generatedEventsMatchRequestedShape() {
    SimulationConfig config = new SimulationConfig(10, 3, 2, 4, 0.25, 123L, 1000L, 5L);

    SimulationData data = DummyDataSimulator.generate(config);

    assertThat(data.events()).hasSize(20);
    assertThat(data.rules()).hasSize(3);
    assertThat(data.queryPoints()).hasSize(2);
    assertThat(data.events().get(0).attributes()).hasSize(3);
  }
}

