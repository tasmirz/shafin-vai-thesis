package com.thesis.topk.dataset;

import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public final class DatasetProviders {
  private static final List<DatasetProvider> PROVIDERS = List.of(
      new SyntheticDatasetProvider(),
      new CsvDatasetProvider(),
      new IntelLabDatasetProvider(),
      new PumpSensorDatasetProvider(),
      new GasSensorDatasetProvider(),
      new AllRawDatasetProvider());

  private DatasetProviders() {
  }

  public static SimulationData generate(Args args) {
    return byName(args.stringValue("dataset", "synthetic")).generate(args.simulationConfig(), args);
  }

  public static DatasetProvider byName(String name) {
    String normalized = name == null || name.isBlank() ? "synthetic" : name.toLowerCase(Locale.ROOT);
    if ("dummy".equals(normalized)) {
      normalized = "synthetic";
    }
    for (DatasetProvider provider : PROVIDERS) {
      if (provider.name().equals(normalized)) {
        return provider;
      }
    }
    throw new IllegalArgumentException(
        "unsupported dataset provider: " + name + " (available: " + names() + ")");
  }

  public static List<DatasetProvider> allRawDatasets() {
    return List.of(byName("intel"), byName("pump"), byName("gas"));
  }

  static Path datasetPath(Args args, String defaultPath) {
    String path = args.stringValue("datasetPath", "").trim();
    if (!path.isEmpty()) {
      return Path.of(path);
    }
    Path local = Path.of(defaultPath);
    if (java.nio.file.Files.exists(local)) {
      return local;
    }
    return Path.of("/opt/spark", defaultPath);
  }

  public static String names() {
    return PROVIDERS.stream().map(DatasetProvider::name).collect(Collectors.joining(","));
  }
}
