package com.pangate.chat.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.config.ChatProperties;
import io.lettuce.core.RedisClient;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** CH3 spec §6.1: the shared connection sets. */
class RedisPresenceIT {

  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static RedisPresence presence;
  static RedisClient raw;

  @BeforeAll
  static void boot() {
    redis.start();
    String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    presence = new RedisPresence(new ChatProperties.Redis(uri, Duration.ofSeconds(1), Duration.ofSeconds(60)));
    raw = RedisClient.create(uri);
  }

  @AfterAll
  static void shutdown() {
    presence.close();
    raw.shutdown();
    redis.stop();
  }

  @Test
  void setsExpireAfter24h() {
    assertThat(presence.add("x1", "n1", 1, "c1")).isTrue();
    assertThat(presence.add("x1", "n1", 1, "c2")).isFalse();
    try (var c = raw.connect()) {
      assertThat(c.sync().pttl("pres:x1")).isBetween(1L, 86_400_000L);
      assertThat(c.sync().pttl("presnode:n1:1")).isBetween(1L, 86_400_000L);
    }
    assertThat(presence.remove("x1", "n1", 1, "c1")).isFalse();
    assertThat(presence.remove("x1", "n1", 1, "c2")).isTrue();
    assertThat(presence.online("x1")).isFalse();
  }

  @Test
  void dropNodeRemovesOnlyThatNode() {
    presence.add("y1", "n7", 3, "a");
    presence.add("y2", "n7", 3, "b");
    presence.add("y2", "n8", 1, "c");
    assertThat(presence.dropNode("n7", 3)).containsExactly("y1");
    assertThat(presence.online("y1")).isFalse();
    assertThat(presence.online("y2")).isTrue();
  }

  @Test
  void eventsGraceAndLastSeen() {
    assertThat(presence.seen("e1")).isFalse();
    assertThat(presence.seen("e1")).isTrue();
    presence.markGone("z1", 42);
    assertThat(presence.gone("z1")).hasValue(42);
    presence.clearGone("z1");
    assertThat(presence.gone("z1")).isEmpty();
    presence.putLastSeen("z1", 99);
    assertThat(presence.lastSeen("z1")).hasValue(99);
  }
}
