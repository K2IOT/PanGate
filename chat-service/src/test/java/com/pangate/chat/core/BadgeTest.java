package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.UserConv;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/** CH4 spec §3.1: the badge is every unread message, muted conversations included. */
class BadgeTest {

  @Test
  void sumOfUnreadNeverNegative() {
    FakeStores stores = new FakeStores();
    Map<String, UserConv> mine = new ConcurrentHashMap<>();
    mine.put("c1", new UserConv("c1", null, 10, 7, 1));
    mine.put("c2", new UserConv("c2", null, 5, 5, 2));
    mine.put("c3", new UserConv("c3", null, 3, 9, 3));      // read ahead of a stale last_seq: 0, not -6
    mine.put("c4", new UserConv("c4", null, 4, 0, 4, 0, Long.MAX_VALUE));   // muted still counts
    stores.userConvs.put("u", mine);
    assertThat(new Badge(stores).total("u")).isEqualTo(3 + 4);
    assertThat(new Badge(stores).total("nobody")).isZero();
  }

  /** Review 2026-11-19 (F4): fan-out may raise the inbox row after the notification is built; the message counts. */
  @Test
  void theMessageBeingNotifiedAlwaysCounts() {
    FakeStores stores = new FakeStores();
    Map<String, UserConv> mine = new ConcurrentHashMap<>();
    mine.put("c1", new UserConv("c1", null, 5, 5, 1));      // everything read; seq 6 not touched in yet
    mine.put("c2", new UserConv("c2", null, 4, 2, 2));
    stores.userConvs.put("u", mine);
    Badge b = new Badge(stores);
    assertThat(b.total("u", "c1", 6)).isEqualTo(1 + 2);
    assertThat(b.total("u", "c2", 3)).as("an older seq changes nothing").isEqualTo(2);
    assertThat(b.total("u", "c9", 7)).as("no inbox row yet: the message alone").isEqualTo(1 + 2);
  }

  // --- perf 3a (spec §3.3): the maintained counter ---

  static final long H = 3_600_000L;
  final FakeStores inbox = new FakeStores();
  final FakeBadges counters = new FakeBadges();
  final java.util.concurrent.atomic.AtomicLong wall = new java.util.concurrent.atomic.AtomicLong(1_000_000_000L);
  final Badge badge = new Badge(inbox, counters, wall::get);

  void row(String user, String conv, com.pangate.contracts.v1.ConversationKind kind, long last, long read) {
    inbox.userConvs.computeIfAbsent(user, k -> new ConcurrentHashMap<>()).put(conv,
        new UserConv(conv, kind, last, read, 1));
  }

  @Test
  void raiseOnMissingHashCreatesNothing() {
    badge.raise("u", "c1", 5, 0);
    badge.raiseAll(java.util.List.of("u"), "c1", 6);
    badge.drop("u", "c1");
    assertThat(counters.hashes).doesNotContainKey("u");
  }

  @Test
  void firstReadRebuildsFromInbox() {
    row("u", "dm", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_DIRECT, 7, 4);
    row("u", "ch", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_CHANNEL, 9, 0);
    assertThat(badge.total("u", null, 0)).isEqualTo(3);
    assertThat(counters.hashes.get("u")).containsKey("_built_at").doesNotContainKey("ch");
  }

  @Test
  void notifiedMessageCountsBeforeInboxRow() {
    row("u", "dm", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_DIRECT, 7, 4);
    badge.total("u", null, 0);
    assertThat(badge.total("u", "dm", 8)).isEqualTo(4);
    assertThat(badge.total("u", "new-dm", 1)).as("a DM created after the hash").isEqualTo(5);
  }

  @Test
  void joinAfterMessageCountsOne() {
    badge.total("u", null, 0);
    badge.raiseAll(java.util.List.of("u"), "g", 101);
    badge.raise("u", "g", 100, 100);
    assertThat(badge.total("u", null, 0)).isEqualTo(1);
    badge.total("v", null, 0);
    badge.raise("v", "g", 100, 100);
    badge.raiseAll(java.util.List.of("v"), "g", 101);
    assertThat(badge.total("v", null, 0)).isEqualTo(1);
  }

  @Test
  void redisDownFallsBackToScan() {
    row("u", "dm", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_DIRECT, 7, 4);
    counters.down = true;
    assertThat(badge.total("u", "dm", 9)).isEqualTo(5);
  }

  @Test
  void failedWriteForgetsHash() {
    badge.total("u", null, 0);
    counters.failNext = 1;
    badge.raise("u", "dm", 3, 0);
    assertThat(counters.hashes).doesNotContainKey("u");
    assertThat(counters.forgotten).containsExactly("u");
    badge.total("v", null, 0);
    counters.down = true;
    badge.raise("v", "dm", 3, 0);   // the forget fails too: still nothing thrown
    badge.drop("v", "dm");
    badge.raiseAll(java.util.List.of("v"), "dm", 4);
  }

  @Test
  void lostWriteHealsAfter24h() {
    row("u", "dm", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_DIRECT, 7, 4);
    assertThat(badge.total("u", null, 0)).isEqualTo(3);
    row("u", "dm", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_DIRECT, 9, 4);   // raise lost
    wall.addAndGet(23 * H);
    assertThat(badge.total("u", null, 0)).isEqualTo(3);
    wall.addAndGet(H + 1);
    assertThat(badge.total("u", null, 0)).isEqualTo(5);
  }

  @Test
  void lostDropHealsAfter24h() {
    row("u", "dm", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_DIRECT, 7, 4);
    row("u", "g", com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_GROUP, 10, 0);
    assertThat(badge.total("u", null, 0)).isEqualTo(13);
    inbox.userConvs.get("u").remove("g");   // left; the drop was lost
    wall.addAndGet(24 * H + 1);
    assertThat(badge.total("u", null, 0)).isEqualTo(3);
    assertThat(counters.hashes.get("u")).doesNotContainKey("g");
  }
}
