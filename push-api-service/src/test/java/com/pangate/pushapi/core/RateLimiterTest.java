package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.PushRequest;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RateLimiterTest {

  final AtomicLong nanos = new AtomicLong();
  final RateLimiter limiter = new RateLimiter(nanos::get);
  final TenantPolicy p = new TenantPolicy(false, true, 10, 20, 2);

  @Test
  void bucketAllowsBurstThenRate() {
    for (int i = 0; i < 20; i++) {
      assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, p)).as("token %d", i).isZero();
    }
    long wait = limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, p);
    assertThat(wait).isBetween(1L, 100_000_000L);
    nanos.addAndGet(100_000_000L);
    assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, p)).isZero();
    assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, p)).isPositive();
  }

  @Test
  void tenantsAndKindsHaveTheirOwnBuckets() {
    for (int i = 0; i < 20; i++) {
      limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, p);
    }
    assertThat(limiter.tryAcquire("t2", PushRequest.Kind.KIND_MESSAGE, p)).isZero();
    assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_BROADCAST, p)).isZero();
    assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_BROADCAST, p)).isZero();
    assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_BROADCAST, p)).isPositive();  // broadcast burst = rate 2
  }

  @Test
  void controlIsNeverLimited() {
    TenantPolicy zero = new TenantPolicy(false, true, 1, 1, 1);
    for (int i = 0; i < 1000; i++) {
      assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_CONTROL, zero)).isZero();
    }
  }

  @Test
  void newRatesApplyToAnExistingBucket() {
    TenantPolicy one = new TenantPolicy(false, true, 1, 1, 1);
    limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, one);
    assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, one)).isPositive();
    nanos.addAndGet(1_000_000_000L);
    TenantPolicy many = new TenantPolicy(false, true, 100, 100, 1);
    assertThat(limiter.tryAcquire("t1", PushRequest.Kind.KIND_MESSAGE, many)).isZero();
  }
}
