package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.chat.config.ChatProperties;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Perf 3a (spec §3.2): one bucket per user per API group, checked before any lookup. */
class ApiLimitsTest {

  final AtomicLong nanos = new AtomicLong();
  final ApiLimits limits = new ApiLimits(ChatProperties.Api.DEFAULTS, nanos::get);

  static final Map<ApiLimits.Bucket, int[]> RATES = Map.of(
      ApiLimits.Bucket.LIST, new int[] {5, 20},
      ApiLimits.Bucket.STATE, new int[] {2, 10},
      ApiLimits.Bucket.SYNC, new int[] {2, 10},
      ApiLimits.Bucket.PRESENCE, new int[] {2, 10},
      ApiLimits.Bucket.HISTORY, new int[] {20, 50},
      ApiLimits.Bucket.MANAGE, new int[] {5, 20},
      ApiLimits.Bucket.OTHER, new int[] {10, 30});

  @Test
  void eachBucketHasItsOwnRateAndBurst() {
    for (ApiLimits.Bucket b : ApiLimits.Bucket.values()) {
      int rate = RATES.get(b)[0];
      int burst = RATES.get(b)[1];
      String user = "u-" + b;
      for (int i = 0; i < burst; i++) {
        limits.check(b, user);
      }
      assertThatThrownBy(() -> limits.check(b, user)).isInstanceOfSatisfying(ChatRefusal.class,
          e -> assertThat(e.code()).isEqualTo(ChatRefusal.RATE_LIMITED));
      nanos.addAndGet(1_000_000_000L);
      for (int i = 0; i < rate; i++) {
        limits.check(b, user);
      }
      assertThatThrownBy(() -> limits.check(b, user)).isInstanceOf(ChatRefusal.class);
    }
  }

  @Test
  void usersDoNotShareBuckets() {
    for (int i = 0; i < 10; i++) {
      limits.check(ApiLimits.Bucket.STATE, "a");
    }
    assertThatThrownBy(() -> limits.check(ApiLimits.Bucket.STATE, "a")).isInstanceOf(ChatRefusal.class);
    limits.check(ApiLimits.Bucket.STATE, "b");
    limits.check(ApiLimits.Bucket.SYNC, "a");
  }

  @Test
  void unlimitedNeverRefuses() {
    ApiLimits none = ApiLimits.unlimited();
    for (int i = 0; i < 10_000; i++) {
      none.check(ApiLimits.Bucket.MANAGE, "a");
    }
  }
}
