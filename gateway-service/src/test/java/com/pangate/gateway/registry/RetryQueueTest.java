package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.metrics.GatewayMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryQueueTest {

  private record Scheduled(Runnable task, long delayMs) {}

  private final List<Scheduled> scheduled = new ArrayList<>();
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  private final RetryQueue queue = new RetryQueue((r, d) -> scheduled.add(new Scheduled(r, d)), 2,
      new GatewayMetrics(meters));

  private void runNext() {
    scheduled.remove(0).task().run();
  }

  private double errors(String reason) {
    var c = meters.find("gw.registry.write.errors").tag("reason", reason).counter();
    return c == null ? 0 : c.count();
  }

  @Test
  void retriesAt100ms1s5sThenDrops() {
    AtomicInteger calls = new AtomicInteger();
    queue.retryLater("register", () -> {
      calls.incrementAndGet();
      return CompletableFuture.failedFuture(new RuntimeException("down"));
    }, () -> true);

    List<Long> delays = new ArrayList<>();
    while (!scheduled.isEmpty()) {
      delays.add(scheduled.get(0).delayMs());
      runNext();
    }

    assertThat(delays).containsExactly(100L, 1_000L, 5_000L);
    assertThat(calls).hasValue(3);
    assertThat(errors("dropped")).isEqualTo(1);
    assertThat(queue.size()).isZero();
  }

  @Test
  void successStopsRetrying() {
    AtomicInteger calls = new AtomicInteger();
    queue.retryLater("register", () -> calls.incrementAndGet() == 1
        ? CompletableFuture.failedFuture(new RuntimeException("x")) : CompletableFuture.completedFuture(1), () -> true);

    runNext();
    runNext();

    assertThat(scheduled).isEmpty();
    assertThat(calls).hasValue(2);
    assertThat(queue.size()).isZero();
  }

  @Test
  void overflowCountsAndRejects() {
    for (int i = 0; i < 3; i++) {
      queue.retryLater("register", () -> CompletableFuture.completedFuture(1), () -> true);
    }

    assertThat(errors("overflow")).isEqualTo(1);
    assertThat(queue.size()).isEqualTo(2);
  }

  @Test
  void retryOfClosedSessionIsSkipped() {
    AtomicInteger calls = new AtomicInteger();
    queue.retryLater("register", () -> {
      calls.incrementAndGet();
      return CompletableFuture.completedFuture(1);
    }, () -> false);

    runNext();

    assertThat(calls).hasValue(0);
    assertThat(queue.size()).isZero();
    assertThat(errors("dropped")).isZero();
  }

  @Test
  void opThrowingSynchronouslyCountsAsFailure() {
    queue.retryLater("register", () -> {
      throw new IllegalStateException("not connected");
    }, () -> true);

    runNext();

    assertThat(scheduled).hasSize(1);
    assertThat(scheduled.get(0).delayMs()).isEqualTo(1_000L);
  }
}
