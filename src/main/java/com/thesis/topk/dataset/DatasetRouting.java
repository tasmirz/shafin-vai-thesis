package com.thesis.topk.dataset;

import com.thesis.topk.simulator.Args;

public final class DatasetRouting {
  private DatasetRouting() {
  }

  public static String mqttTopic(DatasetProvider provider, Args args) {
    if (args.has("topic")) {
      return args.stringValue("topic", provider.mqttTopic());
    }
    return provider.mqttTopic();
  }

  public static String kafkaTopic(DatasetProvider provider, Args args) {
    if (args.has("kafkaTopic")) {
      return args.stringValue("kafkaTopic", provider.kafkaTopic());
    }
    return provider.kafkaTopic();
  }
}
