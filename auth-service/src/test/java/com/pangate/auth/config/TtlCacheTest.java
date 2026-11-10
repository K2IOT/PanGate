package com.pangate.auth.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TtlCacheTest {

  @Test
  void callersDuringASlowReloadGetTheStaleValueInsteadOfWaiting() throws Exception {
    AtomicLong nanos = new AtomicLong();
    AtomicInteger value = new AtomicInteger();
    CountDownLatch inLoad = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    TtlCache<Integer> cache = new TtlCache<>(() -> {
      int v = value.incrementAndGet();
      if (v == 2) {
        inLoad.countDown();
        await(release);
      }
      return v;
    }, Duration.ofSeconds(10), 0, nanos::get);
    assertThat(cache.get()).isEqualTo(1);
    nanos.addAndGet(Duration.ofSeconds(11).toNanos());

    var slow = CompletableFuture.supplyAsync(cache::get);
    assertThat(inLoad.await(5, TimeUnit.SECONDS)).isTrue();
    long t0 = System.nanoTime();
    assertThat(cache.get()).isEqualTo(1);  // stale, not blocked behind the reload
    assertThat(System.nanoTime() - t0).isLessThan(Duration.ofMillis(500).toNanos());
    release.countDown();
    assertThat(slow.get(5, TimeUnit.SECONDS)).isEqualTo(2);
    assertThat(cache.get()).isEqualTo(2);
  }

  private static void await(CountDownLatch l) {
    try {
      l.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
