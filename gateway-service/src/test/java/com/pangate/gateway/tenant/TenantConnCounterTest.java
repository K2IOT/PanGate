package com.pangate.gateway.tenant;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TenantConnCounterTest {

  private final TenantConnCounter c = new TenantConnCounter();

  @Test
  void reserveStopsAtQuotaAndReleaseFreesIt() {
    assertThat(c.tryReserve("t1", 2)).isTrue();
    assertThat(c.tryReserve("t1", 2)).isTrue();
    assertThat(c.tryReserve("t1", 2)).isFalse();
    assertThat(c.count("t1")).isEqualTo(2);
    c.release("t1");
    assertThat(c.tryReserve("t1", 2)).isTrue();
    c.reserveBorrowed("t1");
    assertThat(c.count("t1")).isEqualTo(3);
    assertThat(c.snapshot()).containsEntry("t1", 3);
  }

  @Test
  void releaseToZeroDropsTheEntry() {
    c.tryReserve("t1", 5);
    c.release("t1");
    assertThat(c.snapshot()).doesNotContainKey("t1");
    assertThat(c.count("t1")).isZero();
  }

  @Test
  void concurrentReservesNeverExceedQuota() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(8);
    AtomicInteger ok = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    for (int t = 0; t < 8; t++) {
      pool.execute(() -> {
        try {
          start.await();
        } catch (InterruptedException e) {
          return;
        }
        for (int i = 0; i < 1000; i++) {
          if (c.tryReserve("t1", 100)) {
            ok.incrementAndGet();
          }
        }
      });
    }
    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(ok.get()).isEqualTo(100);
    assertThat(c.count("t1")).isEqualTo(100);
  }

  @Test
  void quotaRoundsUpWithTenPercentHeadroom() {
    assertThat(TenantCapBorrower.quota(1000, 3)).isEqualTo(367);
    assertThat(TenantCapBorrower.quota(2, 1)).isEqualTo(3);
    assertThat(TenantCapBorrower.quota(10, 0)).isEqualTo(11);  // nodes is at least 1
  }
}
