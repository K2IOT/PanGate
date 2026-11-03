package com.pangate.gateway.metrics;

import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.server.TickScheduler;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;

/**
 * Event loop lag (spec §8): every 100 ms each loop gets a no-op task; the delay until it runs is recorded in a ring
 * of the last 100 samples per loop. Reported as p99 seconds per loop.
 */
public final class EventLoopLagProbe {

  private static final int SAMPLES = 100;

  private final LoopRegistry loops;
  private final TickScheduler tick;
  private final LongSupplier nanoClock;
  private final AtomicLongArray[] rings;
  private final AtomicInteger[] counts;

  public EventLoopLagProbe(LoopRegistry loops, TickScheduler tick, LongSupplier nanoClock) {
    this.loops = loops;
    this.tick = tick;
    this.nanoClock = nanoClock;
    this.rings = new AtomicLongArray[loops.size()];
    this.counts = new AtomicInteger[loops.size()];
    for (int i = 0; i < loops.size(); i++) {
      rings[i] = new AtomicLongArray(SAMPLES);
      counts[i] = new AtomicInteger();
    }
  }

  public void start() {
    tick.scheduleAtFixedRate(this::probe, Duration.ofMillis(100));
  }

  private void probe() {
    for (LoopContext c : loops.all()) {
      long t0 = nanoClock.getAsLong();
      int idx = c.index();
      c.eventLoop().execute(() -> {
        int n = counts[idx].getAndIncrement();
        rings[idx].set(n % SAMPLES, nanoClock.getAsLong() - t0);
      });
    }
  }

  public int samples(int loop) {
    return Math.min(counts[loop].get(), SAMPLES);
  }

  public double p99Seconds(int loop) {
    int n = samples(loop);
    if (n == 0) {
      return 0;
    }
    long[] copy = new long[n];
    for (int i = 0; i < n; i++) {
      copy[i] = rings[loop].get(i);
    }
    Arrays.sort(copy);
    return copy[Math.max(0, (int) Math.ceil(0.99 * n) - 1)] / 1e9;
  }

  public double maxP99Seconds() {
    double max = 0;
    for (int i = 0; i < rings.length; i++) {
      max = Math.max(max, p99Seconds(i));
    }
    return max;
  }

  public void bindTo(MeterRegistry registry) {
    for (int i = 0; i < rings.length; i++) {
      int loop = i;
      Gauge.builder("gw.eventloop.lag", this, p -> p.p99Seconds(loop)).baseUnit("seconds")
          .tag("loop", Integer.toString(loop)).register(registry);
    }
  }
}
