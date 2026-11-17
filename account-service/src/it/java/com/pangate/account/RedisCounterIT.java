package com.pangate.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.application.port.Clock;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.infrastructure.redis.RedisAttemptCounter;
import com.pangate.account.support.Containers;
import io.lettuce.core.RedisClient;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RedisCounterIT {

  /** Final review I6: a counter whose expiry was lost (INCR done, PEXPIRE failed) gets one again: no lockout forever. */
  @Test
  void aCounterWithoutExpiryGetsOne() {
    String key = "reg:" + UUID.randomUUID();
    RedisClient raw = RedisClient.create(Containers.redisUri());
    try (var conn = raw.connect();
        RedisAttemptCounter c = new RedisAttemptCounter(Containers.redisUri(), Duration.ofSeconds(1),
            new InMemoryAttemptCounter(Clock.SYSTEM))) {
      conn.sync().set("acct:n:" + key, "7");   // what a failed PEXPIRE leaves behind
      assertThat(c.increment(key, Duration.ofMinutes(5))).isEqualTo(8);
      assertThat(conn.sync().pttl("acct:n:" + key)).isBetween(1L, 300_000L);
    } finally {
      raw.shutdown();
    }
  }

  /** Review round 2, Important 1: a registration slot given back after a failed insert; never below zero. */
  @Test
  void decrementGivesBackOneAndStopsAtZero() {
    String key = "reg:" + UUID.randomUUID();
    try (RedisAttemptCounter c = new RedisAttemptCounter(Containers.redisUri(), Duration.ofSeconds(1),
        new InMemoryAttemptCounter(Clock.SYSTEM))) {
      c.increment(key, Duration.ofMinutes(5));
      c.increment(key, Duration.ofMinutes(5));
      c.decrement(key);
      assertThat(c.count(key)).isEqualTo(1);
      c.decrement(key);
      c.decrement(key);
      assertThat(c.count(key)).isZero();
      c.decrement("reg:" + UUID.randomUUID());   // no window: nothing created
    }
  }
}
