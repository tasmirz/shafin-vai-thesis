package com.thesis.topk.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/** Collects task-level observed Spark cost metrics for finite benchmark runs. */
public final class SparkTaskMetrics extends SparkListener {
  private final AtomicLong taskCount = new AtomicLong();
  private final AtomicLong executorRunTimeMs = new AtomicLong();
  private final AtomicLong executorCpuTimeNanos = new AtomicLong();
  private final AtomicLong jvmGcTimeMs = new AtomicLong();
  private final AtomicLong shuffleWriteBytes = new AtomicLong();
  private final AtomicLong shuffleWriteRecords = new AtomicLong();
  private final AtomicLong shuffleWriteTimeNanos = new AtomicLong();
  private final AtomicLong peakTaskRunTimeMs = new AtomicLong();
  private final List<Long> taskRunTimesMs = new ArrayList<>();

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    TaskMetrics metrics = taskEnd.taskMetrics();
    if (metrics == null) {
      return;
    }
    taskCount.incrementAndGet();
    executorRunTimeMs.addAndGet(metrics.executorRunTime());
    executorCpuTimeNanos.addAndGet(metrics.executorCpuTime());
    jvmGcTimeMs.addAndGet(metrics.jvmGCTime());
    peakTaskRunTimeMs.accumulateAndGet(metrics.executorRunTime(), Math::max);
    synchronized (taskRunTimesMs) {
      taskRunTimesMs.add(metrics.executorRunTime());
    }
    ShuffleWriteMetrics shuffle = metrics.shuffleWriteMetrics();
    if (shuffle != null) {
      shuffleWriteBytes.addAndGet(shuffle.bytesWritten());
      shuffleWriteRecords.addAndGet(shuffle.recordsWritten());
      shuffleWriteTimeNanos.addAndGet(shuffle.writeTime());
    }
  }

  public Snapshot snapshot() {
    return new Snapshot(
        taskCount.get(),
        executorRunTimeMs.get(),
        executorCpuTimeNanos.get(),
        jvmGcTimeMs.get(),
        shuffleWriteBytes.get(),
        shuffleWriteRecords.get(),
        shuffleWriteTimeNanos.get(),
        peakTaskRunTimeMs.get());
  }

  public Snapshot delta(Snapshot before) {
    Snapshot current = snapshot();
    long peak = 0L;
    synchronized (taskRunTimesMs) {
      for (int index = (int) before.taskCount(); index < taskRunTimesMs.size(); index++) {
        peak = Math.max(peak, taskRunTimesMs.get(index));
      }
    }
    return new Snapshot(
        current.taskCount - before.taskCount,
        current.executorRunTimeMs - before.executorRunTimeMs,
        current.executorCpuTimeNanos - before.executorCpuTimeNanos,
        current.jvmGcTimeMs - before.jvmGcTimeMs,
        current.shuffleWriteBytes - before.shuffleWriteBytes,
        current.shuffleWriteRecords - before.shuffleWriteRecords,
        current.shuffleWriteTimeNanos - before.shuffleWriteTimeNanos,
        peak);
  }

  public record Snapshot(
      long taskCount,
      long executorRunTimeMs,
      long executorCpuTimeNanos,
      long jvmGcTimeMs,
      long shuffleWriteBytes,
      long shuffleWriteRecords,
      long shuffleWriteTimeNanos,
      long peakTaskRunTimeMs) implements Serializable {
    public double stragglerRatio() {
      if (taskCount == 0 || executorRunTimeMs == 0) {
        return 0.0;
      }
      return peakTaskRunTimeMs / ((double) executorRunTimeMs / taskCount);
    }
  }
}
