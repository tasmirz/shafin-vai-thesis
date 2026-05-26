package com.thesis.topk.algorithm.variant;

import java.io.Serializable;

/**
 * One experimental PTD treatment.
 *
 * <p>Implementations describe which independently measurable optimization phases are active.
 * The Spark engine remains responsible for exact refinement and validation.</p>
 */
public interface PtdAlgorithm extends Serializable {
  String id();

  String displayName();

  boolean dscpEnabled();

  boolean aesEnabled();

  String purpose();
}
