package com.pangate.chat.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.config.ChatProperties;
import com.pangate.contracts.v1.PresenceWatch;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** CH3 spec §6.3: watches in Redis expire, are replaced per device, and index open conversations. */
class RedisWatchersIT {

  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static RedisPresence w;

  @BeforeAll
  static void boot() {
    redis.start();
    w = new RedisPresence(new ChatProperties.Redis("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
        Duration.ofSeconds(1), Duration.ofSeconds(60)));
  }

  @AfterAll
  static void shutdown() {
    w.close();
    redis.stop();
  }

  static PresenceWatch.Entry e(String user, String conv) {
    return PresenceWatch.Entry.newBuilder().setUserId(user).setConversationId(conv).build();
  }

  @Test
  void expiredWatchersFallOff() throws Exception {
    long now = System.currentTimeMillis();
    w.replace("v1", "d1", List.of(e("t1", "C1")), now + 1_000);
    assertThat(w.watchersOf("t1", now)).containsExactly("v1/d1/C1");
    Thread.sleep(1_500);
    assertThat(w.watchersOf("t1", System.currentTimeMillis())).isEmpty();
  }

  @Test
  void replaceDropsTheOldList() {
    long now = System.currentTimeMillis();
    w.replace("v2", "d1", List.of(e("t2", "C2"), e("t3", "C2")), now + 60_000);
    w.replace("v2", "d1", List.of(e("t3", "C3")), now + 60_000);
    assertThat(w.watchersOf("t2", now)).isEmpty();
    assertThat(w.watchersOf("t3", now)).containsExactly("v2/d1/C3");
    assertThat(w.devicesWithOpen("C2")).isEmpty();
  }

  @Test
  void devicesWithOpenFollowsEntries() {
    long now = System.currentTimeMillis();
    w.replace("v3", "dA", List.of(e("t4", "C4")), now + 60_000);
    w.replace("v4", "dB", List.of(e("t4", "C4"), e("t5", "C5")), now + 60_000);
    assertThat(w.devicesWithOpen("C4")).containsExactlyInAnyOrder("v3/dA", "v4/dB");
    w.replace("v4", "dB", List.of(), now + 60_000);
    assertThat(w.devicesWithOpen("C4")).containsExactly("v3/dA");
  }

  /** Final review I8: a connected device re-watching every 4 min keeps its own connection set alive past 24 h. */
  @Test
  void rewatchRenewsTheViewersConnectionSet() {
    w.add("rv1", "n1", 1, "c1");
    io.lettuce.core.RedisClient raw = io.lettuce.core.RedisClient.create("redis://" + redis.getHost() + ":"
        + redis.getMappedPort(6379));
    try (var c = raw.connect()) {
      c.sync().pexpire("pres:rv1", 2_000);
      w.replace("rv1", "d1", List.of(e("t9", "C9")), System.currentTimeMillis() + 60_000);
      assertThat(c.sync().pttl("pres:rv1")).isGreaterThan(3_600_000L);
    } finally {
      raw.shutdown();
    }
  }
}
