package com.thesis.topk.dataset;

import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class GasSensorDatasetProvider implements DatasetProvider {
  @Override
  public String name() {
    return "gas";
  }

  @Override
  public SimulationData generate(SimulationConfig config, Args args) {
    Path path = DatasetProviders.datasetPath(args, "datasets-raw/gas+sensors+for+home+activity+monitoring.zip");
    int maxEvents = RawDatasetSupport.maxEvents(config, args);
    try {
      byte[] nestedZip = nestedDatasetZip(path);
      try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(nestedZip))) {
        ZipEntry entry;
        while ((entry = zip.getNextEntry()) != null) {
          if (entry.getName().endsWith(".dat")) {
            var reader = new BufferedReader(new InputStreamReader(zip));
            var events = RawDatasetSupport.readWhitespace(reader, maxEvents, this::map);
            return RawDatasetSupport.data(events, 10);
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("failed to read gas dataset: " + path, e);
    }
    throw new IllegalArgumentException("gas dataset nested zip has no dat file: " + path);
  }

  private byte[] nestedDatasetZip(Path path) throws IOException {
    try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(path))) {
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        if ("HT_Sensor_dataset.zip".equals(entry.getName())) {
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          zip.transferTo(out);
          return out.toByteArray();
        }
      }
    }
    throw new IllegalArgumentException("gas dataset outer zip has no HT_Sensor_dataset.zip: " + path);
  }

  private RawEvent map(String[] columns, int emittedIndex) {
    if (columns.length < 12) {
      return null;
    }
    double[] attributes = new double[10];
    for (int i = 0; i < attributes.length; i++) {
      attributes[i] = RawDatasetSupport.value(columns[i + 2]);
    }
    long eventTime = 1_420_070_400_000L + Math.round(RawDatasetSupport.value(columns[1]) * 3_600_000.0);
    return RawDatasetSupport.event("gas-id-" + columns[0], "gas", eventTime, attributes);
  }
}
