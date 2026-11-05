package com.pangate.gateway.shed;

/**
 * Process CPU averaged over a sliding window (100 samples at 100 ms = 10 s, like the lag p99 window, spec §7.3).
 * A single JIT or GC spike must not move the ladder. Single-threaded (gw-tick).
 */
public final class CpuWindow {

  private final double[] ring;
  private int next;
  private int count;
  private double sum;

  public CpuWindow(int samples) {
    this.ring = new double[samples];
  }

  public void record(double load) {
    double v = Math.max(0, load);
    if (count == ring.length) {
      sum -= ring[next];
    } else {
      count++;
    }
    ring[next] = v;
    sum += v;
    next = (next + 1) % ring.length;
  }

  public double average() {
    return count == 0 ? 0 : sum / count;
  }
}
