package com.pangate.chat.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.BadgeRow;
import io.lettuce.core.RedisClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Perf 3a (spec §3.3): the badge counters' Lua scripts against a real Redis. */
class RedisBadgesIT {

  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static RedisBadges badges;
  static RedisClient raw;
  static final long NOW = 1_800_000_000_000L;

  @BeforeAll
  static void boot() {
    redis.start();
    String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    badges = new RedisBadges(new ChatProperties.Redis(uri, Duration.ofSeconds(2), Duration.ofSeconds(60)));
    raw = RedisClient.create(uri);
  }

  @AfterAll
  static void shutdown() {
    badges.close();
    raw.shutdown();
    redis.stop();
  }

  /** A rebuild nothing races: prepare, then rebuild with the version it answered. */
  static void build(String user, List<BadgeRow> rows, long now) {
    assertThat(badges.rebuild(user, rows, now, badges.prepareRebuild(user))).isTrue();
  }

  static String hget(String user, String field) {
    try (var c = raw.connect()) {
      return c.sync().hget("badge:" + user, field);
    }
  }

  static long exists(String user) {
    try (var c = raw.connect()) {
      return c.sync().exists("badge:" + user);
    }
  }

  @Test
  void raiseIsIdempotentAndNeverLowers() {
    build("r1", List.of(), NOW);
    badges.raise("r1", "C", 10, 0);
    badges.raise("r1", "C", 10, 0);
    assertThat(badges.totalWith("r1", null, 0, NOW)).isEqualTo(10);
    badges.raise("r1", "C", 5, 3);
    assertThat(badges.totalWith("r1", null, 0, NOW)).isEqualTo(7);
    badges.raise("r1", "C", 0, 10);
    assertThat(badges.totalWith("r1", null, 0, NOW)).isZero();
    assertThat(hget("r1", "C")).isEqualTo("10:10");
  }

  @Test
  void raiseAndDropOnMissingHashCreateNothing() {
    badges.raise("m1", "C", 5, 0);
    badges.raiseAll(List.of("m1"), "C", 6);
    badges.drop("m1", "C");
    assertThat(exists("m1")).isZero();
  }

  @Test
  void totalWithIsMinusOneWhenMissingOrStale() {
    assertThat(badges.totalWith("s1", null, 0, NOW)).isEqualTo(-1);
    build("s1", List.of(new BadgeRow("C", 4, 1)), NOW);
    assertThat(badges.totalWith("s1", null, 0, NOW + 1)).isEqualTo(-1);
    assertThat(badges.totalWith("s1", null, 0, NOW)).isEqualTo(3);
    assertThat(badges.totalWith("s1", "C", 6, NOW)).as("the notified message counts").isEqualTo(5);
    assertThat(badges.totalWith("s1", "D", 1, NOW)).as("a conversation not seen yet").isEqualTo(6);
    assertThat(hget("s1", "D")).as("counted, not kept").isNull();
  }

  @Test
  void rebuildMergesWithMaxAndDropsMissingFields() {
    build("g1", List.of(new BadgeRow("A", 10, 2), new BadgeRow("B", 5, 5)), NOW);
    build("g1", List.of(new BadgeRow("A", 8, 4), new BadgeRow("C", 3, 0)), NOW + 5);
    assertThat(hget("g1", "A")).isEqualTo("10:4");
    assertThat(hget("g1", "B")).as("not in the inbox: left").isEqualTo("L");
    assertThat(hget("g1", "C")).isEqualTo("3:0");
    assertThat(hget("g1", "_total")).isEqualTo("9");
    assertThat(hget("g1", "_built_at")).isEqualTo(Long.toString(NOW + 5));
  }

  @Test
  void dropSubtractsTheUnread() {
    build("d1", List.of(new BadgeRow("A", 10, 2), new BadgeRow("B", 5, 1)), NOW);
    badges.drop("d1", "A");
    assertThat(badges.totalWith("d1", null, 0, NOW)).isEqualTo(4);
    badges.drop("d1", "A");
    assertThat(badges.totalWith("d1", null, 0, NOW)).isEqualTo(4);
  }

  /** Final review I1: a left conversation keeps a mark that raises ignore until the user joins again. */
  @Test
  void leftConversationIgnoresRaisesUntilJoin() {
    build("j1", List.of(new BadgeRow("A", 10, 10)), NOW);
    badges.drop("j1", "A");
    assertThat(hget("j1", "A")).isEqualTo("L");
    badges.raise("j1", "A", 5000, 0);
    badges.raiseAll(List.of("j1"), "A", 5001);
    assertThat(badges.totalWith("j1", "A", 5002, NOW)).isZero();
    badges.join("j1", "A", 6000);
    assertThat(hget("j1", "A")).isEqualTo("6000:6000");
    badges.raiseAll(List.of("j1"), "A", 6001);
    assertThat(badges.totalWith("j1", null, 0, NOW)).isEqualTo(1);
    build("j1", List.of(new BadgeRow("A", 6001, 6000)), NOW);
    assertThat(badges.totalWith("j1", null, 0, NOW)).isEqualTo(1);
    badges.join("missing", "A", 1);
    assertThat(exists("missing")).isZero();
  }

  @Test
  void largeSeqStaysDecimal() {
    build("l1", List.of(), NOW);
    badges.raise("l1", "C", 1_000_000_000_000_000L, 0);
    assertThat(hget("l1", "C")).isEqualTo("1000000000000000:0");
    assertThat(badges.totalWith("l1", null, 0, NOW)).isEqualTo(1_000_000_000_000_000L);
  }

  @Test
  void raiseAllPipelinesFiveHundredUsers() {
    List<String> users = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      users.add("p" + i);
      build("p" + i, List.of(), NOW);
    }
    long t0 = System.nanoTime();
    badges.raiseAll(users, "G", 1);
    assertThat(System.nanoTime() - t0).isLessThan(1_000_000_000L);
    for (String u : users) {
      assertThat(badges.totalWith(u, null, 0, NOW)).isEqualTo(1);
    }
  }

  @Test
  void ttlIsThirtyDays() {
    build("t1", List.of(), NOW);
    badges.raise("t1", "C", 1, 0);
    try (var c = raw.connect()) {
      assertThat(c.sync().pttl("badge:t1")).isGreaterThan(Duration.ofDays(29).toMillis())
          .isLessThanOrEqualTo(Duration.ofDays(30).toMillis());
    }
  }

  @Test
  void rebuildOfTenThousandRows() {
    List<BadgeRow> rows = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      rows.add(new BadgeRow("c" + i, 3, 1));
    }
    build("k1", rows, NOW);
    assertThat(badges.totalWith("k1", null, 0, NOW)).isEqualTo(20_000);
  }

  @Test
  void forgetDeletesTheHash() {
    build("f1", List.of(), NOW);
    badges.forget(List.of("f1"));
    assertThat(exists("f1")).isZero();
  }

  /** Codex review of PR #10: a join or leave after prepareRebuild makes the rebuild refuse and change nothing. */
  @Test
  void rebuildRefusesWhenMembershipMovedSincePrepare() {
    long v = badges.prepareRebuild("v1");
    assertThat(hget("v1", "_built_at")).as("placeholder reads as stale").isEqualTo("0");
    assertThat(badges.totalWith("v1", null, 0, NOW)).isEqualTo(-1);
    badges.raise("v1", "G", 7, 0);   // lands in the placeholder
    badges.drop("v1", "G");
    assertThat(badges.rebuild("v1", List.of(new BadgeRow("G", 7, 0)), NOW, v)).isFalse();
    assertThat(hget("v1", "G")).as("the left mark survives").isEqualTo("L");
    assertThat(hget("v1", "_built_at")).isEqualTo("0");
    long v2 = badges.prepareRebuild("v1");
    assertThat(v2).isEqualTo(v + 1);
    assertThat(badges.rebuild("v1", List.of(), NOW, v2)).isTrue();
    assertThat(badges.totalWith("v1", null, 0, NOW)).isZero();
  }
}
