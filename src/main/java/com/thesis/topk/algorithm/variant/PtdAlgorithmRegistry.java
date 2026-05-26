package com.thesis.topk.algorithm.variant;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Registry of named treatments used in reproducible ablation runs. */
public final class PtdAlgorithmRegistry {
  public static final String DEFAULT_ID = "aes-dscp";

  private static final Map<String, PtdAlgorithm> ALGORITHMS = createAlgorithms();

  private PtdAlgorithmRegistry() {
  }

  public static PtdAlgorithm defaultAlgorithm() {
    return require(DEFAULT_ID);
  }

  public static PtdAlgorithm require(String identifier) {
    String id = normalize(identifier);
    PtdAlgorithm algorithm = ALGORITHMS.get(id);
    if (algorithm == null) {
      throw new IllegalArgumentException(
          "Unsupported PTD algorithm '" + identifier + "'. Available: "
              + String.join(", ", availableIds()));
    }
    return algorithm;
  }

  public static List<PtdAlgorithm> available() {
    return List.copyOf(ALGORITHMS.values());
  }

  public static List<String> availableIds() {
    return available().stream().map(PtdAlgorithm::id).toList();
  }

  private static Map<String, PtdAlgorithm> createAlgorithms() {
    Map<String, PtdAlgorithm> algorithms = new LinkedHashMap<>();
    register(algorithms, new ConfiguredAlgorithm(
        "baseline",
        "Baseline distributed PTD",
        false,
        false,
        "No AES and no DSCP; paper comparison control."));
    register(algorithms, new ConfiguredAlgorithm(
        "dscp-only",
        "DSCP-only",
        true,
        false,
        "Candidate-pruning ablation with baseline emissions."));
    register(algorithms, new ConfiguredAlgorithm(
        "aes-only",
        "AES-only",
        false,
        true,
        "Aggregated-emission ablation without candidate pruning."));
    register(algorithms, new ConfiguredAlgorithm(
        DEFAULT_ID,
        "AES + DSCP",
        true,
        true,
        "Full named method: candidate pruning plus aggregated emission."));
    return Collections.unmodifiableMap(algorithms);
  }

  private static void register(Map<String, PtdAlgorithm> algorithms, PtdAlgorithm algorithm) {
    algorithms.put(algorithm.id(), algorithm);
  }

  private static String normalize(String identifier) {
    String id = identifier == null ? DEFAULT_ID : identifier.strip().toLowerCase(Locale.ROOT);
    return switch (id) {
      case "full", "aes+dscp", "aes_dscp", "aesdscp" -> DEFAULT_ID;
      case "dscp", "dscp_only" -> "dscp-only";
      case "aes", "aes_only" -> "aes-only";
      default -> id;
    };
  }

  private record ConfiguredAlgorithm(
      String id,
      String displayName,
      boolean dscpEnabled,
      boolean aesEnabled,
      String purpose) implements PtdAlgorithm {
  }
}
