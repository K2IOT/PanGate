package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TokenBucketTest {

  @Test
  void bucketRefillsAtRate() {
    TokenBucket b = new TokenBucket(10, 2, 0);

    assertThat(b.tryAcquire(0)).isTrue();
    assertThat(b.tryAcquire(0)).isTrue();
    assertThat(b.tryAcquire(0)).isFalse();
    assertThat(b.tryAcquire(TimeUnit.MILLISECONDS.toNanos(100))).isTrue();
    assertThat(b.tryAcquire(TimeUnit.MILLISECONDS.toNanos(100))).isFalse();
  }

  @Test
  void refillIsCappedAtBurst() {
    TokenBucket b = new TokenBucket(10, 2, 0);

    long later = TimeUnit.SECONDS.toNanos(60);
    assertThat(b.tryAcquire(later)).isTrue();
    assertThat(b.tryAcquire(later)).isTrue();
    assertThat(b.tryAcquire(later)).isFalse();
  }

  @Test
  void setRateLowersBurstImmediately() {
    TokenBucket b = new TokenBucket(10, 5, 0);

    b.setRate(10, 1);

    assertThat(b.tryAcquire(0)).isTrue();
    assertThat(b.tryAcquire(0)).isFalse();
  }
}
