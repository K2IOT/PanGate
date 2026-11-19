package com.pangate.chat.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.config.ChatProperties;
import io.lettuce.core.RedisClient;
import java.time.Duration;
import java.util.List;
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
    assertThat(presence.handled("e1")).isFalse();
    presence.markHandled("e1");
    assertThat(presence.handled("e1")).isTrue();
    presence.markGone("z1", 42);
    assertThat(presence.gone("z1")).hasValue(42);
    presence.clearGone("z1");
    assertThat(presence.gone("z1")).isEmpty();
    presence.putLastSeen("z1", 99);
    assertThat(presence.lastSeen("z1")).hasValue(99);
  }

  /** Final review I7: after NODE_DOWN, a late CONNECTED of that node is ignored. */
  @Test
  void deadNodeConnectionsAreIgnored() {
    presence.dropNode("n9", 4);
    assertThat(presence.add("dz", "n9", 4, "c1")).isFalse();
    assertThat(presence.online("dz")).isFalse();
    assertThat(presence.add("dz", "n9", 5, "c1")).isTrue();   // the node's next epoch is alive
  }

  /** Final review I6: the grace queue is shared and each due grace is claimed once. */
  @Test
  void graceQueueIsClaimedOnce() {
    presence.scheduleGrace("gq1", 1_000);
    presence.scheduleGrace("gq2", 9_000);
    assertThat(presence.dueGraces(5_000)).contains("gq1").doesNotContain("gq2");
    assertThat(presence.claimGrace("gq1")).isTrue();
    assertThat(presence.claimGrace("gq1")).isFalse();
    presence.cancelGrace("gq2");
    assertThat(presence.dueGraces(10_000)).doesNotContain("gq2");
  }

  /** CH4 spec §4.2: the online users of a page of followers. */
  @Test
  void onlineOfReturnsTheOnlineOnes() {
    presence.add("on1", "n9", 1, "k1");
    presence.add("on2", "n9", 1, "k2");
    assertThat(presence.onlineOf(java.util.List.of("on1", "off1", "on2"))).containsExactlyInAnyOrder("on1", "on2");
  }

  /** CH4 deferred minor: a whole page (1 000 followers) answered from one pipelined batch. */
  @Test
  void onlineOfAPageOfAThousand() {
    java.util.List<String> page = new java.util.ArrayList<>();
    java.util.Set<String> on = new java.util.HashSet<>();
    for (int i = 0; i < 1_000; i++) {
      String u = "pg" + i;
      page.add(u);
      if (i % 7 == 0) {
        presence.add(u, "n9", 1, "c" + i);
        on.add(u);
      }
    }
    assertThat(presence.onlineOf(page)).isEqualTo(on);
  }

  /** Review 2026-11-19 (F6): open channels per user, read for a page of users in one pipelined round. */
  @Test
  void openChannels() {
    presence.channelOpened("o1", "chA");
    presence.channelOpened("o1", "chB");
    presence.channelOpened("o2", "chA");
    assertThat(presence.haveOpen("chA", List.of("o1", "o2", "o3"))).containsExactlyInAnyOrder("o1", "o2");
    presence.channelClosed("o1", "chA");
    assertThat(presence.haveOpen("chA", List.of("o1", "o2"))).containsExactly("o2");
    presence.clearOpenChannels("o1");
    assertThat(presence.haveOpen("chB", List.of("o1"))).isEmpty();
    assertThat(presence.haveOpen("chA", List.of())).isEmpty();
  }
}
