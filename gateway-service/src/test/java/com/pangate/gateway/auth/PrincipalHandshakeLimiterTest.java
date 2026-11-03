package com.pangate.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PrincipalHandshakeLimiterTest {

  private final AtomicLong clock = new AtomicLong(1_000_000L);
  private final PrincipalHandshakeLimiter limiter = new PrincipalHandshakeLimiter(20, clock::get);

  @Test
  void limiterSlidingWindow() {
    for (int i = 0; i < 20; i++) {
      assertThat(limiter.tryAcquire("t1:u1")).isTrue();
      clock.addAndGet(100);
    }
    assertThat(limiter.tryAcquire("t1:u1")).isFalse();
    assertThat(limiter.tryAcquire("t1:u2")).isTrue();

    clock.addAndGet(60_000);
    assertThat(limiter.tryAcquire("t1:u1")).isTrue();
  }

  @Test
  void idlePrincipalsAreForgotten() {
    limiter.tryAcquire("t1:old");
    clock.addAndGet(121_000);

    for (int i = 0; i < 2_000; i++) {
      limiter.tryAcquire("t1:new" + (i % 3));
    }
    assertThat(limiter.trackedPrincipals()).as("tryAcquire never sweeps on the event loop").isEqualTo(4);

    limiter.sweepIdle();

    assertThat(limiter.trackedPrincipals()).isEqualTo(3);
  }
}
