package com.pangate.notify.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/** Offline notifications spec §5.1: tokens not registered again for 180 days go. */
class PurgeIdleTokensTest {

  @Test
  void cutsAt180DaysBeforeNow() {
    FakeTokenStore store = new FakeTokenStore();
    Instant now = Instant.parse("2026-11-17T00:00:00Z");
    new PurgeIdleTokens(store, Duration.ofDays(180), Clock.fixed(now, ZoneOffset.UTC)).run();
    assertThat(store.calls).containsExactly("purgeIdle " + now.minus(Duration.ofDays(180)));
  }
}
