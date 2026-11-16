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
}
