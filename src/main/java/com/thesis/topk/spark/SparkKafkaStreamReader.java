package com.thesis.topk.spark;

import com.thesis.topk.io.RawEventJsonSerde;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Reads the finite Kafka snapshot used by a reproducible streaming benchmark through Spark
 * Structured Streaming.
 *
 * <p>The MQTT publisher and EMQX Kafka action run before this reader. Available-now processing
 * consumes all records available when the query starts, stores raw events for a single exact PTD
 * evaluation, then terminates. This avoids comparing moving windows during a fixed benchmark.</p>
 */
public final class SparkKafkaStreamReader {
  private SparkKafkaStreamReader() {
  }

  public static List<RawEvent> readAvailable(
      SparkSession spark,
      Args args,
      List<String> topics,
      int expectedMessages) {
    String bootstrap = args.stringValue("kafkaBootstrap", "localhost:9092");
    String checkpoint = args.stringValue(
        "checkpointLocation",
        "/tmp/probabilistic-topk-spark-checkpoints/" + args.stringValue("kafkaGroupId", "default"));
    long timeoutMs = args.longValue("kafkaReadTimeoutMs", 120_000L);
    List<RawEvent> events = new ArrayList<>();

    Dataset<Row> values = spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", String.join(",", topics))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .selectExpr("CAST(value AS STRING) AS value");

    try {
      StreamingQuery query = values.writeStream()
          .queryName("ptd-kafka-bounded-ingestion")
          .option("checkpointLocation", checkpoint)
          .trigger(Trigger.AvailableNow())
          .foreachBatch((batch, batchId) -> {
            for (Row row : batch.collectAsList()) {
              events.add(RawEventJsonSerde.fromJson(row.getString(0)));
            }
          })
          .start();
      if (!query.awaitTermination(timeoutMs)) {
        query.stop();
        throw new IllegalStateException("Timed out reading Kafka stream after " + timeoutMs + " ms");
      }
    } catch (StreamingQueryException | TimeoutException e) {
      throw new IllegalStateException("Spark Structured Streaming Kafka ingestion failed", e);
    }

    if (events.isEmpty()) {
      throw new IllegalStateException("Spark Kafka stream read no events from topics " + topics);
    }
    if (events.size() < expectedMessages) {
      System.err.printf(
          "WARN sparkKafkaRead topics=%s expectedMessages=%d actualMessages=%d timeoutMs=%d%n",
          topics,
          expectedMessages,
          events.size(),
          timeoutMs);
    }
    System.out.printf(
        "sparkKafkaRead reader=structured-streaming trigger=available-now topics=%s "
            + "expectedMessages=%d actualMessages=%d bootstrap=%s checkpoint=%s%n",
        topics,
        expectedMessages,
        events.size(),
        bootstrap,
        checkpoint);
    return List.copyOf(events.subList(0, Math.min(events.size(), expectedMessages)));
  }
}
