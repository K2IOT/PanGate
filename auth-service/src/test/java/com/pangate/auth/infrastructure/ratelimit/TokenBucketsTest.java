package com.pangate.auth.infrastructure.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TokenBucketsTest {

  private final AtomicLong nanos = new AtomicLong(1_000_000_000L);
  private final TokenBuckets buckets = new TokenBuckets(Map.of("token", new TokenBuckets.Limit(10, 20)), nanos::get);

  @Test
  void bucketRefillsAndReportsRetryAfter() {
    for (int i = 0; i < 20; i++) {
      assertThat(buckets.tryAcquire("token", "c1")).as("call " + i).isEmpty();
    }
    var denied = buckets.tryAcquire("token", "c1");
    assertThat(denied).isPresent();
    assertThat(denied.getAsLong()).isBetween(1L, 100L);          // one token per 100 ms
    assertThat(buckets.tryAcquire("token", "c2")).isEmpty();      // per key

    nanos.addAndGet(Duration.ofMillis(100).toNanos());
    assertThat(buckets.tryAcquire("token", "c1")).isEmpty();
    assertThat(buckets.tryAcquire("token", "c1")).isPresent();

    nanos.addAndGet(Duration.ofSeconds(10).toNanos());           // refill caps at burst
    for (int i = 0; i < 20; i++) {
      assertThat(buckets.tryAcquire("token", "c1")).isEmpty();
    }
    assertThat(buckets.tryAcquire("token", "c1")).isPresent();
  }

  @Test
  void unknownBucketIsUnlimited() {
    assertThat(buckets.tryAcquire("nope", "x")).isEmpty();
  }

  @Test
  void failedAuthBlocksTheSourceAfterTheThreshold() {
    var failures = new FailedAuthTracker(20, Duration.ofSeconds(60), nanos::get);
    for (int i = 0; i < 20; i++) {
      failures.recordFailure("10.0.0.1");
    }
    assertThat(failures.blockedFor("10.0.0.1")).isEmpty();
    failures.recordFailure("10.0.0.1");                           // 21st within a minute
    assertThat(failures.blockedFor("10.0.0.1").getAsLong()).isBetween(59_000L, 60_000L);
    assertThat(failures.blockedFor("10.0.0.2")).isEmpty();
    nanos.addAndGet(Duration.ofSeconds(61).toNanos());
    assertThat(failures.blockedFor("10.0.0.1")).isEmpty();
  }

  @Test
  void failuresOutsideTheMinuteDoNotCount() {
    var failures = new FailedAuthTracker(20, Duration.ofSeconds(60), nanos::get);
    for (int i = 0; i < 20; i++) {
      failures.recordFailure("ip");
    }
    nanos.addAndGet(Duration.ofSeconds(61).toNanos());
    failures.recordFailure("ip");
    assertThat(failures.blockedFor("ip")).isEmpty();
  }

  @Test
  void aFullTrackerEvictsIdleSourcesButKeepsBlockedOnes() {
    var failures = new FailedAuthTracker(2, Duration.ofSeconds(60), nanos::get, 10);
    for (int i = 0; i < 3; i++) {
      failures.recordFailure("attacker");
    }
    assertThat(failures.blockedFor("attacker")).isPresent();
    for (int i = 0; i < 50; i++) {
      failures.recordFailure("10.1.0." + i);  // floods the map
    }
    assertThat(failures.blockedFor("attacker")).isPresent();
  }

  @Test
  void aFullBucketMapKeepsDrainedBuckets() {
    var b = new TokenBuckets(java.util.Map.of("token", new TokenBuckets.Limit(1, 1)), nanos::get, 10);
    assertThat(b.tryAcquire("token", "busy")).isEmpty();
    assertThat(b.tryAcquire("token", "busy")).isPresent();
    for (int i = 0; i < 50; i++) {
      b.tryAcquire("token", "k" + i);
    }
    assertThat(b.tryAcquire("token", "busy")).isPresent();  // not reset to a full bucket
  }
}
