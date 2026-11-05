package com.pangate.gateway.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.control.TenantPolicy;
import com.pangate.gateway.session.SessionRateLimiter.Verdict;
import com.pangate.gateway.test.LoopFixture;
import com.pangate.gateway.test.TestLimits;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class SessionRateLimiterTest {

  private final LoopFixture f = new LoopFixture();
  private final Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
  private final TenantPolicy policy = TenantPolicy.defaults(TestLimits.defaults());

  private static long nanos(long sec) {
    return sec * 1_000_000_000L;
  }

  /** Consumes until the first non-OK verdict within one second. */
  private Verdict exhaust(long sec) {
    for (int i = 0; i < 1_000; i++) {
      Verdict v = SessionRateLimiter.check(s, policy, nanos(sec), sec);
      if (v != Verdict.OK) {
        return v;
      }
    }
    throw new AssertionError("never limited");
  }

  @Test
  void burst100ThenLimited() {
    for (int i = 0; i < 100; i++) {
      assertThat(SessionRateLimiter.check(s, policy, nanos(10), 10)).isEqualTo(Verdict.OK);
    }

    assertThat(SessionRateLimiter.check(s, policy, nanos(10), 10)).isEqualTo(Verdict.LIMITED);
  }

  @Test
  void refillsAtRatePerSecond() {
    exhaust(10);

    for (int i = 0; i < 25; i++) {
      assertThat(SessionRateLimiter.check(s, policy, nanos(10) + 500_000_000L, 10)).isEqualTo(Verdict.OK);
    }
    assertThat(SessionRateLimiter.check(s, policy, nanos(10) + 500_000_000L, 10)).isEqualTo(Verdict.LIMITED);
  }

  @Test
  void threeConsecutiveViolatingSecondsClose() {
    assertThat(exhaust(10)).isEqualTo(Verdict.LIMITED);
    assertThat(SessionRateLimiter.check(s, policy, nanos(10), 10)).isEqualTo(Verdict.LIMITED);
    assertThat(exhaust(11)).isEqualTo(Verdict.LIMITED);
    assertThat(exhaust(12)).isEqualTo(Verdict.CLOSE);
  }

  @Test
  void gapResetsStrikes() {
    exhaust(10);
    exhaust(11);

    assertThat(exhaust(13)).isEqualTo(Verdict.LIMITED);
    assertThat(s.strikes()).isEqualTo(1);
  }
}
