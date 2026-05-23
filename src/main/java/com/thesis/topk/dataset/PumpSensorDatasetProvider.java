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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class PumpSensorDatasetProvider implements DatasetProvider {
  @Override
  public String name() {
    return "pump";
  }

  @Override
  public SimulationData generate(SimulationConfig config, Args args) {
    Path path = DatasetProviders.datasetPath(args, "datasets-raw/pump_sensor_data.zip");
    int maxEvents = RawDatasetSupport.maxEvents(config, args);
    try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(path))) {
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        if (entry.getName().endsWith(".csv")) {
          return readCsv(new BufferedReader(new InputStreamReader(zip)), maxEvents);
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("failed to read pump dataset: " + path, e);
    }
    throw new IllegalArgumentException("pump dataset zip has no csv file: " + path);
  }

  private SimulationData readCsv(BufferedReader reader, int maxEvents) throws IOException {
    String headerLine = reader.readLine();
    if (headerLine == null) {
      throw new IllegalArgumentException("pump dataset csv is empty");
    }
    String[] header = headerLine.split(",", -1);
    List<Integer> sensorColumns = new ArrayList<>();
    for (int i = 0; i < header.length; i++) {
      if (header[i].startsWith("sensor_")) {
        sensorColumns.add(i);
      }
    }

    List<RawEvent> events = new ArrayList<>(maxEvents);
    String line;
    while ((line = reader.readLine()) != null && events.size() < maxEvents) {
      String[] cells = line.split(",", -1);
      if (cells.length < header.length) {
        continue;
      }
      double[] attributes = new double[sensorColumns.size()];
      for (int i = 0; i < sensorColumns.size(); i++) {
        attributes[i] = RawDatasetSupport.value(cells[sensorColumns.get(i)]);
      }
      long eventTime = RawDatasetSupport.pumpTimestamp(cells[1]);
      String status = cells[cells.length - 1].isBlank() ? "UNKNOWN" : cells[cells.length - 1];
      returnEvent(events, eventTime, attributes, status);
    }
    return RawDatasetSupport.data(events, sensorColumns.size());
  }

  private static void returnEvent(List<RawEvent> events, long eventTime, double[] attributes, String status) {
    events.add(RawDatasetSupport.event("pump-row-" + events.size(), "pump-" + status.toLowerCase(), eventTime, attributes));
  }
}
