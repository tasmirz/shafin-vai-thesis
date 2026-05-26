package com.thesis.topk.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import org.junit.jupiter.api.Test;

class RawEventJsonSerdeTest {
  @Test
  void serializesMissingAttributesAsNullAndReadsThemBackAsNan() {
    RawEvent event = new RawEvent(
        "obj-1",
        "q0",
        1234L,
        new double[] {0.4, Double.NaN},
        new boolean[] {false, true},
        OpType.UPSERT);

    String json = RawEventJsonSerde.toJson(event);
    RawEvent parsed = RawEventJsonSerde.fromJson(json);

    assertThat(json).contains("\"attributes\":[0.4,null]");
    assertThat(parsed.objectId()).isEqualTo("obj-1");
    assertThat(parsed.queryId()).isEqualTo("q0");
    assertThat(parsed.eventTime()).isEqualTo(1234L);
    assertThat(parsed.missingMask()).containsExactly(false, true);
    assertThat(parsed.attributes()[0]).isEqualTo(0.4);
    assertThat(Double.isNaN(parsed.attributes()[1])).isTrue();
  }

  @Test
  void retainsCuratedInstanceMetadata() {
    RawEvent event = new RawEvent(
        "road-1", "road-1-i0", "q0", 1234L, 0.25, 4,
        new double[] {10.0, 20.0}, new boolean[] {false, false},
        new double[] {9.0, 19.0}, new double[] {11.0, 21.0}, OpType.UPSERT);

    RawEvent parsed = RawEventJsonSerde.fromJson(RawEventJsonSerde.toJson(event));

    assertThat(parsed.instanceId()).isEqualTo("road-1-i0");
    assertThat(parsed.appearanceProbability()).isEqualTo(0.25);
    assertThat(parsed.serverPartition()).isEqualTo(4);
    assertThat(parsed.mbrMin()).containsExactly(9.0, 19.0);
    assertThat(parsed.mbrMax()).containsExactly(11.0, 21.0);
    assertThat(parsed.hasMbr()).isTrue();
  }
}
