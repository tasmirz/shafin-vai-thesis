package com.thesis.topk.dataset;

import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

public final class IntelLabDatasetProvider implements DatasetProvider {
  @Override
  public String name() {
    return "intel";
  }

  @Override
  public SimulationData generate(SimulationConfig config, Args args) {
    Path path = DatasetProviders.datasetPath(args, "datasets-raw/intel_lab_data.gz");
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))))) {
      var events = RawDatasetSupport.readWhitespace(reader, RawDatasetSupport.maxEvents(config, args), this::map);
      return RawDatasetSupport.data(events, 4);
    } catch (IOException e) {
      throw new IllegalArgumentException("failed to read Intel dataset: " + path, e);
    }
  }

  private RawEvent map(String[] columns, int emittedIndex) {
    if (columns.length < 8) {
      return null;
    }
    long eventTime = RawDatasetSupport.intelTimestamp(columns[0], columns[1]);
    String moteId = columns[3];
    double[] attributes = {
        RawDatasetSupport.value(columns[4]),
        RawDatasetSupport.value(columns[5]),
        RawDatasetSupport.value(columns[6]),
        RawDatasetSupport.value(columns[7])
    };
    return RawDatasetSupport.event("intel-mote-" + moteId, "intel", eventTime, attributes);
  }
}
