package com.pangate.account.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class InMemoryAttemptCounterTest {

  /** Final review I7: during a long Redis outage the fallback must not keep every expired key forever. */
  @Test
  void expiredKeysAreDroppedWithoutAnyJob() {
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-16T10:00:00Z"));
    InMemoryAttemptCounter c = new InMemoryAttemptCounter(now::get);
    for (int i = 0; i < 10_000; i++) {
      c.increment("k" + i, Duration.ofMinutes(1));
      c.block("b" + i, Duration.ofMinutes(1));
    }
    now.set(now.get().plus(Duration.ofMinutes(2)));
    for (int i = 0; i < 2_048; i++) {
      c.increment("fresh" + i, Duration.ofMinutes(1));
    }
    assertThat(c.size()).isLessThanOrEqualTo(2_048);
  }
}
