package com.pangate.gateway.shed;

import com.pangate.gateway.config.GatewayProperties;
import java.util.function.LongSupplier;

/**
 * New-connection admission (spec §7.1): one token bucket per event loop (rate / loops, no shared atomics), with a
 * linear ramp from {@code rampFrom} to the base rate after the node becomes ready. Each loop calls
 * {@link #tryAdmit(int)} only for its own index.
 */
public final class AdmissionController {

  private final GatewayProperties.Admission cfg;
  private final int loops;
  private final LongSupplier nanoClock;
  private final TokenBucket[] buckets;
  private volatile long readyAtNanos = Long.MIN_VALUE;
  private volatile int overrideRate;

  public AdmissionController(GatewayProperties.Admission cfg, int loops, LongSupplier nanoClock) {
    this.cfg = cfg;
    this.loops = loops;
    this.nanoClock = nanoClock;
    this.buckets = new TokenBucket[loops];
    long now = nanoClock.getAsLong();
    for (int i = 0; i < loops; i++) {
      buckets[i] = new TokenBucket(cfg.rampFrom() / (double) loops, burstPerLoop(), now);
    }
  }

  /** Starts the ramp; before this the rate stays at {@code rampFrom}. */
  public void markReady() {
    readyAtNanos = nanoClock.getAsLong();
  }

  /** Kill switch admission.ratePerNode (M3); 0 clears the override. */
  public void overrideBaseRate(int rate) {
    overrideRate = rate;
  }

  public double effectiveRate() {
    double base = overrideRate > 0 ? overrideRate : cfg.rate();
    double from = Math.min(cfg.rampFrom(), base);
    long readyAt = readyAtNanos;
    if (readyAt == Long.MIN_VALUE) {
      return from;
    }
    double fraction = (nanoClock.getAsLong() - readyAt) / (double) cfg.rampDuration().toNanos();
    return fraction >= 1.0 ? base : from + (base - from) * Math.max(0.0, fraction);
  }

  public boolean tryAdmit(int loopIdx) {
    TokenBucket b = buckets[loopIdx];
    b.setRate(effectiveRate() / loops, burstPerLoop());
    return b.tryAcquire(nanoClock.getAsLong());
  }

  private double burstPerLoop() {
    return Math.max(1.0, cfg.burst() / (double) loops);
  }
}
