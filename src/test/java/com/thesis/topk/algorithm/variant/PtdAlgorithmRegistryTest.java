package com.thesis.topk.algorithm.variant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PtdAlgorithmRegistryTest {
  @Test
  void exposesPaperControlAndAblationTreatments() {
    assertThat(PtdAlgorithmRegistry.availableIds())
        .containsExactly("baseline", "dscp-only", "aes-only", "aes-dscp");
    assertThat(PtdAlgorithmRegistry.require("baseline").dscpEnabled()).isFalse();
    assertThat(PtdAlgorithmRegistry.require("baseline").aesEnabled()).isFalse();
    assertThat(PtdAlgorithmRegistry.require("dscp-only").dscpEnabled()).isTrue();
    assertThat(PtdAlgorithmRegistry.require("dscp-only").aesEnabled()).isFalse();
    assertThat(PtdAlgorithmRegistry.require("aes-only").dscpEnabled()).isFalse();
    assertThat(PtdAlgorithmRegistry.require("aes-only").aesEnabled()).isTrue();
    assertThat(PtdAlgorithmRegistry.require("AES+DSCP").id()).isEqualTo("aes-dscp");
  }

  @Test
  void rejectsUnregisteredTreatment() {
    assertThatThrownBy(() -> PtdAlgorithmRegistry.require("approximate"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("baseline")
        .hasMessageContaining("aes-dscp");
  }
}
