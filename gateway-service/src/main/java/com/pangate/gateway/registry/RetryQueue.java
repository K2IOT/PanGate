package com.pangate.gateway.registry;

import com.pangate.gateway.metrics.GatewayMetrics;
import io.micrometer.core.instrument.Counter;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Node-wide retry for failed registry writes (spec §6.2): at most {@code capacity} pending entries, retried after
 * 100 ms, 1 s and 5 s, then dropped. A retry whose session no longer wants it (closed meanwhile) is skipped.
 */
public final class RetryQueue {

  /** Schedules a task after a delay; production uses gw-tick. */
  @FunctionalInterface
  public interface Scheduler {
    void schedule(Runnable task, long delayMs);
  }

  private static final long[] DELAYS_MS = {100, 1_000, 5_000};

  private final Scheduler scheduler;
  private final int capacity;
  private final AtomicInteger size = new AtomicInteger();
  private final Counter overflow;
  private final Counter dropped;

  public RetryQueue(Scheduler scheduler, int capacity, GatewayMetrics metrics) {
    this.scheduler = scheduler;
    this.capacity = capacity;
    this.overflow = metrics.counter("gw.registry.write.errors", "reason", "overflow");
    this.dropped = metrics.counter("gw.registry.write.errors", "reason", "dropped");
  }

  public void retryLater(String kind, Supplier<CompletionStage<?>> op, BooleanSupplier stillWanted) {
    if (size.incrementAndGet() > capacity) {
      size.decrementAndGet();
      overflow.increment();
      return;
    }
    scheduler.schedule(() -> attempt(op, stillWanted, 0), DELAYS_MS[0]);
  }

  private void attempt(Supplier<CompletionStage<?>> op, BooleanSupplier stillWanted, int n) {
    if (!stillWanted.getAsBoolean()) {
      size.decrementAndGet();
      return;
    }
    CompletionStage<?> st;
    try {
      st = op.get();
    } catch (RuntimeException e) {
      failed(op, stillWanted, n);
      return;
    }
    st.whenComplete((v, e) -> {
      if (e == null) {
        size.decrementAndGet();
      } else {
        failed(op, stillWanted, n);
      }
    });
  }

  private void failed(Supplier<CompletionStage<?>> op, BooleanSupplier stillWanted, int n) {
    if (n + 1 >= DELAYS_MS.length) {
      size.decrementAndGet();
      dropped.increment();
      return;
    }
    scheduler.schedule(() -> attempt(op, stillWanted, n + 1), DELAYS_MS[n + 1]);
  }

  public int size() {
    return size.get();
  }
}
