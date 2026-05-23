package com.thesis.topk.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasetProvidersTest {
  @TempDir
  Path tempDir;

  @Test
  void defaultsToSyntheticProvider() {
    SimulationConfig config = new SimulationConfig(4, 2, 2, 2, 0.25, 7L, 1000L, 5L);

    SimulationData data = DatasetProviders.byName("synthetic").generate(config, Args.parse(new String[0]));

    assertThat(data.events()).hasSize(8);
    assertThat(data.rules()).hasSize(2);
    assertThat(data.queryPoints()).containsKeys("q0", "q1");
  }

  @Test
  void csvProviderLoadsFutureDatasetFiles() throws Exception {
    Path csv = tempDir.resolve("events.csv");
    Files.writeString(csv, """
        objectId,queryId,eventTime,opType,a0,a1,a2
        o-1,q0,1000,UPSERT,0.10,,0.30
        o-2,q0,1010,UPSERT,0.20,null,?
        """);

    Args args = Args.parse(new String[] {"--dataset=csv", "--datasetPath=" + csv});
    SimulationData data = DatasetProviders.generate(args);

    assertThat(data.events()).hasSize(2);
    assertThat(data.events().get(0).attributes()).hasSize(3);
    assertThat(data.events().get(0).missingMask()).containsExactly(false, true, false);
    assertThat(data.events().get(1).missingMask()).containsExactly(false, true, true);
    assertThat(data.rules()).hasSize(3);
    assertThat(data.queryPoints()).containsKey("q0");
  }

  @Test
  void csvProviderRequiresDatasetPath() {
    Args args = Args.parse(new String[] {"--dataset=csv"});

    assertThatThrownBy(() -> DatasetProviders.generate(args))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--datasetPath");
  }

  @Test
  void rawDatasetProvidersPreprocessKnownArchives() {
    SimulationConfig config = new SimulationConfig(2, 4, 1, 2, 0.25, 7L, 1000L, 5L);

    for (String dataset : new String[] {"intel", "pump", "gas"}) {
      Args args = Args.parse(new String[] {"--dataset=" + dataset, "--maxEvents=2"});
      SimulationData data = DatasetProviders.generate(args);

      assertThat(data.events()).hasSize(2);
      assertThat(data.rules()).isNotEmpty();
      assertThat(data.queryPoints()).isNotEmpty();
    }

    SimulationData all = DatasetProviders.generate(Args.parse(new String[] {"--dataset=all", "--maxEvents=2"}));
    assertThat(all.events()).hasSize(6);
    assertThat(all.queryPoints()).containsKeys("intel", "gas");
  }
}
