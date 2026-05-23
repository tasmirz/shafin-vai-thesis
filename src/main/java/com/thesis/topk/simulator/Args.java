package com.thesis.topk.simulator;

import java.util.HashMap;
import java.util.Map;

public final class Args {
  private final Map<String, String> values;

  private Args(Map<String, String> values) {
    this.values = values;
  }

  public static Args parse(String[] args) {
    Map<String, String> values = new HashMap<>();
    for (String arg : args) {
      if (!arg.startsWith("--") || !arg.contains("=")) {
        continue;
      }
      int idx = arg.indexOf('=');
      values.put(arg.substring(2, idx), arg.substring(idx + 1));
    }
    return new Args(values);
  }

  public int intValue(String key, int defaultValue) {
    return values.containsKey(key) ? Integer.parseInt(values.get(key)) : defaultValue;
  }

  public long longValue(String key, long defaultValue) {
    return values.containsKey(key) ? Long.parseLong(values.get(key)) : defaultValue;
  }

  public boolean booleanValue(String key, boolean defaultValue) {
    return values.containsKey(key) ? Boolean.parseBoolean(values.get(key)) : defaultValue;
  }

  public String stringValue(String key, String defaultValue) {
    return values.getOrDefault(key, defaultValue);
  }

  public boolean has(String key) {
    return values.containsKey(key);
  }

  public double doubleValue(String key, double defaultValue) {
    return values.containsKey(key) ? Double.parseDouble(values.get(key)) : defaultValue;
  }

  public SimulationConfig simulationConfig() {
    SimulationConfig defaults = SimulationConfig.defaults();
    return new SimulationConfig(
        intValue("objects", defaults.objects()),
        intValue("dimensions", defaults.dimensions()),
        intValue("queries", defaults.queries()),
        intValue("k", defaults.k()),
        doubleValue("missingRate", defaults.missingRate()),
        longValue("seed", defaults.seed()),
        longValue("startTimeMs", defaults.startTimeMs()),
        longValue("eventGapMs", defaults.eventGapMs()));
  }
}
