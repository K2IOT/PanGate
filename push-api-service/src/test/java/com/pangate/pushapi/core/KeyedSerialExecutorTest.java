package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class KeyedSerialExecutorTest {

  @Test
  void sameKeyRunsInOrderOtherKeysInParallel() throws Exception {
    var pool = Executors.newVirtualThreadPerTaskExecutor();
    KeyedSerialExecutor ex = new KeyedSerialExecutor(pool);
    List<Integer> order = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch release = new CountDownLatch(1);
    List<CompletableFuture<Integer>> a = new ArrayList<>();
    a.add(ex.submit("a", () -> {
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      order.add(0);
      return 0;
    }));
    for (int i = 1; i < 50; i++) {
      int n = i;
      a.add(ex.submit("a", () -> {
        order.add(n);
        return n;
      }));
    }
    // "b" is not held up by the blocked "a"
    assertThat(ex.submit("b", () -> 7).get(2, TimeUnit.SECONDS)).isEqualTo(7);
    assertThat(order).isEmpty();
    release.countDown();
    CompletableFuture.allOf(a.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      expected.add(i);
    }
    assertThat(order).containsExactlyElementsOf(expected);
    assertThat(ex.pendingKeys()).isZero();
    pool.shutdown();
  }

  @Test
  void aFailureDoesNotStopTheNextOfTheSameKey() throws Exception {
    KeyedSerialExecutor ex = new KeyedSerialExecutor(Executors.newVirtualThreadPerTaskExecutor());
    CompletableFuture<Integer> bad = ex.submit("k", () -> {
      throw new IllegalStateException("boom");
    });
    assertThat(ex.submit("k", () -> 2).get(2, TimeUnit.SECONDS)).isEqualTo(2);
    assertThat(bad).isCompletedExceptionally();
  }
}
