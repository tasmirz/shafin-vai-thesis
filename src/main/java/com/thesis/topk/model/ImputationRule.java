package com.thesis.topk.model;

import java.io.Serializable;

public class ImputationRule implements Serializable {
  private int dimension;
  private double lowValue;
  private double highValue;
  private double lowProbability;

  public ImputationRule() {
  }

  public ImputationRule(int dimension, double lowValue, double highValue, double lowProbability) {
    if (lowProbability < 0.0 || lowProbability > 1.0) {
      throw new IllegalArgumentException("lowProbability must be in [0, 1]");
    }
    this.dimension = dimension;
    this.lowValue = lowValue;
    this.highValue = highValue;
    this.lowProbability = lowProbability;
  }

  public int dimension() {
    return dimension;
  }

  public double lowValue() {
    return lowValue;
  }

  public double highValue() {
    return highValue;
  }

  public double lowProbability() {
    return lowProbability;
  }

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  public double getLowValue() {
    return lowValue;
  }

  public void setLowValue(double lowValue) {
    this.lowValue = lowValue;
  }

  public double getHighValue() {
    return highValue;
  }

  public void setHighValue(double highValue) {
    this.highValue = highValue;
  }

  public double getLowProbability() {
    return lowProbability;
  }

  public void setLowProbability(double lowProbability) {
    this.lowProbability = lowProbability;
  }
}
