package com.pangate.gateway.loop;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Spec §11, §3.5: one wheel tick with 100k nodes whose deadlines are spread over 60 s; due nodes are rescheduled
 * 60 s ahead, as heartbeats do. One op = one 1 s tick (about 1/60 of the nodes due).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class DeadlineWheelBench {

  private static final class Node extends WheelNode {}

  @Param({"100000"})
  public int nodes;

  private DeadlineWheel wheel;
  private long now;

  @Setup(Level.Trial)
  public void setUp() {
    now = 1_000_000L;
    wheel = new DeadlineWheel(128, now);
    for (int i = 0; i < nodes; i++) {
      wheel.schedule(new Node(), now + 1 + (i % 60));
    }
  }

  @Benchmark
  public int tick() {
    long t = ++now;
    wheel.advanceTo(t, n -> wheel.schedule(n, t + 60));
    return wheel.size();
  }
}
