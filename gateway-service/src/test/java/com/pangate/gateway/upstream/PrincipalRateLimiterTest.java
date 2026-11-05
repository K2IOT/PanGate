package com.pangate.gateway.upstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PrincipalRateLimiterTest {

  private final AtomicLong nanos = new AtomicLong();
  private final PrincipalRateLimiter r = new PrincipalRateLimiter(nanos::get);

  @Test
  void principalsHaveSeparateBuckets() {
    assertThat(r.tryAcquire("t1:a", 1, 1)).isTrue();
    assertThat(r.tryAcquire("t1:a", 1, 1)).isFalse();
    assertThat(r.tryAcquire("t1:b", 1, 1)).isTrue();
  }

  @Test
  void idleBucketsAreEvicted() {
    r.tryAcquire("t1:a", 1, 1);
    nanos.addAndGet(PrincipalRateLimiter.IDLE_NANOS + 1);
    r.evictIdle();
    assertThat(r.size()).isZero();
  }

  @Test
  void zeroRateMeansUnlimited() {
    for (int i = 0; i < 100; i++) {
      assertThat(r.tryAcquire("t1:a", 0, 0)).isTrue();
    }
    assertThat(r.size()).isZero();
  }
}
