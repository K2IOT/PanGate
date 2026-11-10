package com.pangate.auth.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class JtiRevocationCacheTest {

  private final AtomicLong nanos = new AtomicLong();
  private final Set<String> revoked = new HashSet<>();
  private final AtomicInteger lookups = new AtomicInteger();
  private final JtiRevocationCache cache = new JtiRevocationCache(j -> {
    lookups.incrementAndGet();
    return revoked.contains(j);
  }, Duration.ofSeconds(10), 3, nanos::get);

  @Test
  void looksUpOneJtiAtATimeAndCachesItForTheTtl() {
    assertThat(cache.isRevoked("a")).isFalse();
    revoked.add("a");
    assertThat(cache.isRevoked("a")).isFalse();  // cached
    assertThat(lookups).hasValue(1);
    nanos.addAndGet(Duration.ofSeconds(11).toNanos());
    assertThat(cache.isRevoked("a")).isTrue();
    assertThat(lookups).hasValue(2);
  }

  @Test
  void boundedSize() {
    for (String j : new String[] {"a", "b", "c", "d"}) {
      cache.isRevoked(j);
    }
    assertThat(cache.size()).isEqualTo(3);
  }

  @Test
  void lookupFailureFallsBackToTheLastKnownAnswer() {
    revoked.add("a");
    assertThat(cache.isRevoked("a")).isTrue();
    nanos.addAndGet(Duration.ofSeconds(11).toNanos());
    var failing = new JtiRevocationCache(j -> {
      throw new IllegalStateException("db down");
    }, Duration.ofSeconds(10), 3, nanos::get);
    assertThat(failing.isRevoked("x")).isFalse();
  }
}
