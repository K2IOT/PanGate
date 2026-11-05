package com.pangate.gateway.tenant;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.metrics.GatewayMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class TenantCapTest {

  private final TenantConnCounter counter = new TenantConnCounter();
  private final List<Integer> sent = new ArrayList<>();
  private final List<CompletableFuture<Boolean>> answers = new ArrayList<>();
  private final TenantCap cap = new TenantCap(counter, () -> 1, (t, max, ownAfter) -> {
    sent.add(ownAfter);
    CompletableFuture<Boolean> f = new CompletableFuture<>();
    answers.add(f);
    return f;
  }, new GatewayMetrics(new SimpleMeterRegistry()));

  @Test
  void concurrentBorrowsReserveFirstAndSendIncreasingTotals() {
    for (int i = 0; i < 3; i++) {
      counter.tryReserve("t1", 3);
    }
    var a = cap.borrow("t1", 2);
    var b = cap.borrow("t1", 2);
    assertThat(sent).containsExactly(4, 5);  // each in-flight borrow already counts
    assertThat(counter.count("t1")).isEqualTo(5);

    answers.get(0).complete(false);
    answers.get(1).complete(true);
    assertThat(a.toCompletableFuture().join()).isFalse();
    assertThat(b.toCompletableFuture().join()).isTrue();
    assertThat(counter.count("t1")).isEqualTo(4);  // the denied slot was given back
  }

  @Test
  void failedBorrowGivesTheSlotBack() {
    counter.tryReserve("t1", 1);
    var a = cap.borrow("t1", 1);
    answers.get(0).completeExceptionally(new IllegalStateException("redis down"));
    assertThat(a.toCompletableFuture().join()).isFalse();
    assertThat(counter.count("t1")).isEqualTo(1);
  }
}
