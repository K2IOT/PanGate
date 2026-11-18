package com.pangate.notify.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.notify.support.Containers;
import io.lettuce.core.RedisClient;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Review of PR #7 (Codex): a counter left without a TTL must not rate-limit its device for good. */
class RedisRateLimiterIT {

  static RedisClient client;
  static LazyRedis redis;

  @BeforeAll
  static void start() {
    client = RedisClient.create(Containers.redisUri());
    redis = new LazyRedis(client);
  }

  @AfterAll
  static void stop() {
    redis.close();
    client.shutdown();
  }

  @Test
  void aCounterWithoutExpiryGetsOneOnTheNextHit() {
    String key = "dev-" + UUID.randomUUID();
    // INCR went through but EXPIRE did not (the process died between the two calls)
    for (int i = 0; i < 20; i++) {
      redis.sync().incr("notifyreg:" + key);
    }
    assertThat(redis.sync().ttl("notifyreg:" + key)).isEqualTo(-1);
    RedisRateLimiter limiter = new RedisRateLimiter(redis);
    assertThat(limiter.tryAcquire(key, 10)).isFalse();
    assertThat(redis.sync().ttl("notifyreg:" + key)).isBetween(1L, 60L);
  }

  @Test
  void firstHitSetsTheMinuteWindow() {
    String key = "dev-" + UUID.randomUUID();
    RedisRateLimiter limiter = new RedisRateLimiter(redis);
    assertThat(limiter.tryAcquire(key, 1)).isTrue();
    assertThat(limiter.tryAcquire(key, 1)).isFalse();
    assertThat(redis.sync().ttl("notifyreg:" + key)).isBetween(1L, 60L);
  }
}
