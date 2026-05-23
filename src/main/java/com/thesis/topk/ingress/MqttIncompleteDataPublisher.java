package com.thesis.topk.ingress;

import com.thesis.topk.dataset.DatasetProviders;
import com.thesis.topk.dataset.DatasetProvider;
import com.thesis.topk.dataset.DatasetRouting;
import com.thesis.topk.io.RawEventJsonSerde;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public final class MqttIncompleteDataPublisher {
  private MqttIncompleteDataPublisher() {
  }

  public static void main(String[] argv) throws Exception {
    Args args = Args.parse(argv);
    SimulationConfig config = args.simulationConfig();
    String mqttUrl = args.stringValue("mqttUrl", "tcp://localhost:1883");
    String clientId = args.stringValue("clientId", "incomplete-data-publisher");
    int qos = args.intValue("qos", 1);
    int repeat = args.intValue("repeat", 0);
    double ratePerSecond = args.doubleValue("ratePerSecond", 10.0);
    long delayMs = ratePerSecond <= 0 ? 0L : Math.max(1L, Math.round(1000.0 / ratePerSecond));
    MqttConnectOptions options = new MqttConnectOptions();
    options.setAutomaticReconnect(true);
    options.setCleanSession(true);

    try (MqttClient client = new MqttClient(mqttUrl, clientId, new MemoryPersistence())) {
      client.connect(options);
      int rounds = 0;
      do {
        publishConfiguredDatasets(client, args, config, qos, delayMs);
        rounds++;
      } while (repeat == 0 || rounds < repeat);
      client.disconnect();
    }
  }

  private static void publishConfiguredDatasets(MqttClient client, Args args, SimulationConfig config, int qos,
      long delayMs) throws Exception {
    String dataset = args.stringValue("dataset", "synthetic");
    if ("all".equalsIgnoreCase(dataset)) {
      for (DatasetProvider provider : DatasetProviders.allRawDatasets()) {
        SimulationData data = provider.generate(config, args);
        publishBatch(client, provider.mqttTopic(), qos, data.events(), delayMs);
      }
      return;
    }

    DatasetProvider provider = DatasetProviders.byName(dataset);
    SimulationData data = provider.generate(config, args);
    publishBatch(client, DatasetRouting.mqttTopic(provider, args), qos, data.events(), delayMs);
  }

  private static void publishBatch(MqttClient client, String topic, int qos, List<RawEvent> events,
      long delayMs) throws Exception {
    for (RawEvent event : events) {
      MqttMessage message = new MqttMessage(RawEventJsonSerde.toJson(event).getBytes(StandardCharsets.UTF_8));
      message.setQos(qos);
      client.publish(topic, message);
      if (delayMs > 0) {
        Thread.sleep(delayMs);
      }
    }
  }
}
