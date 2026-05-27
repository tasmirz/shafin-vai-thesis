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
  void csvProviderRetainsNormalizedPaperInstancesAndAssignedPartitions() throws Exception {
    Path csv = tempDir.resolve("paper.csv");
    Files.writeString(csv, """
        objectId,instanceId,probability,serverId,queryId,eventTime,opType,queryA0,queryA1,a0,a1,mbrMinX,mbrMinY,mbrMaxX,mbrMaxY
        road-1,road-1-i0,0.4,2,q0,1000,UPSERT,5.0,6.0,1.0,2.0,1.0,2.0,1.1,2.1
        road-1,road-1-i1,0.6,2,q0,1001,UPSERT,5.0,6.0,1.1,2.1,1.0,2.0,1.1,2.1
        """);

    SimulationData data = DatasetProviders.generate(
        Args.parse(new String[] {"--dataset=csv", "--datasetPath=" + csv}));

    assertThat(data.events()).hasSize(2);
    assertThat(data.events().get(0).appearanceProbability()).isEqualTo(0.4);
    assertThat(data.events().get(0).serverPartition()).isEqualTo(2);
    assertThat(data.events().get(0).hasMbr()).isTrue();
    assertThat(data.events().get(0).mbrMin()).containsExactly(1.0, 2.0);
    assertThat(data.queryPoints().get("q0").coordinates()).containsExactly(5.0, 6.0);
  }

  @Test
  void csvProviderLoadsSidecarQueriesWithoutDuplicatingPaperInstances() throws Exception {
    Path csv = tempDir.resolve("paper.csv");
    Path queries = tempDir.resolve("queries.csv");
    Files.writeString(csv, """
        objectId,instanceId,probability,serverId,queryId,eventTime,opType,queryA0,queryA1,a0,a1,mbrMinX,mbrMinY,mbrMaxX,mbrMaxY
        road-1,road-1-i0,0.4,2,q0,1000,UPSERT,5.0,6.0,1.0,2.0,1.0,2.0,1.1,2.1
        road-1,road-1-i1,0.6,2,q0,1001,UPSERT,5.0,6.0,1.1,2.1,1.0,2.0,1.1,2.1
        """);
    Files.writeString(queries, """
        queryId,queryA0,queryA1
        q0,5.0,6.0
        q1,7.0,8.0
        """);

    SimulationData data = DatasetProviders.generate(
        Args.parse(new String[] {
            "--dataset=csv", "--datasetPath=" + csv, "--querySetPath=" + queries
        }));

    assertThat(data.events()).hasSize(2);
    assertThat(data.events()).extracting(event -> event.queryId())
        .containsOnly(CsvDatasetProvider.SHARED_QUERY_ID);
    assertThat(data.queryPoints()).containsKeys("q0", "q1");
    assertThat(data.queryPoints().get("q1").coordinates()).containsExactly(7.0, 8.0);
  }

  @Test
  void csvProviderRejectsNonNormalizedPaperObject() throws Exception {
    Path csv = tempDir.resolve("invalid-paper.csv");
    Files.writeString(csv, """
        objectId,instanceId,probability,serverId,queryId,eventTime,opType,a0,a1
        road-1,road-1-i0,0.4,2,q0,1000,UPSERT,1.0,2.0
        road-1,road-1-i1,0.4,2,q0,1001,UPSERT,1.1,2.1
        """);

    assertThatThrownBy(() -> DatasetProviders.generate(
        Args.parse(new String[] {"--dataset=csv", "--datasetPath=" + csv})))
        .hasMessageContaining("probability does not sum to 1");
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
