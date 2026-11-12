package com.pangate.chat.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.Ids;
import com.pangate.contracts.v1.ConversationKind;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

class ChatStoreIT {

  static final ScyllaDBContainer scylla = new ScyllaDBContainer("scylladb/scylla:6.2.3");
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static CqlSession session;
  static ScyllaChatStore store;
  static RedisChatCache cache;

  @BeforeAll
  static void boot() {
    scylla.start();
    redis.start();
    session = ScyllaSession.connect(new ChatProperties.Scylla(
        List.of(scylla.getHost() + ":" + scylla.getMappedPort(9042)), "datacenter1", "chat", Duration.ofSeconds(5), 1));
    SchemaMigrator.migrate(session, "chat");
    store = new ScyllaChatStore(session, "chat");
    cache = new RedisChatCache(new ChatProperties.Redis("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
        Duration.ofSeconds(1), Duration.ofSeconds(60)));
  }

  @AfterAll
  static void shutdown() {
    cache.close();
    session.close();
    redis.stop();
    scylla.stop();
  }

  static String conv(ConversationKind kind, String... members) {
    String id = Ids.next();
    store.create(new Conversation(id, kind, "t", members[0], 0, false, members.length),
        java.util.Arrays.stream(members).map(u -> new Member(u, Role.MEMBER)).toList());
    return id;
  }

  static StoredMessage msg(String conv, long seq) {
    return new StoredMessage(conv, seq, Ids.next(), "u1", Ids.next(), "m" + seq, "", List.of(), 1_000 + seq);
  }

  @Test
  void reserveSeqIsACompareAndSet() {
    String id = conv(ConversationKind.CONVERSATION_KIND_GROUP, "u1", "u2");
    assertThat(store.reserveSeq(id, 0, 3)).isTrue();
    assertThat(store.lastSeq(id)).isEqualTo(3);
    assertThat(store.reserveSeq(id, 0, 1)).isFalse();
    assertThat(store.lastSeq(id)).isEqualTo(3);
  }

  /** Review Focus 4. */
  @Test
  void pagingAcrossBucketEdgeHasNoGapOrRepeat() {
    String id = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, "u1");
    for (long s = 9_995; s <= 10_005; s++) {
      store.write(msg(id, s));
    }
    assertThat(store.before(id, 10_001, 5)).extracting(StoredMessage::convSeq)
        .containsExactly(10_000L, 9_999L, 9_998L, 9_997L, 9_996L);
    assertThat(store.after(id, 9_999, 5)).extracting(StoredMessage::convSeq)
        .containsExactly(10_000L, 10_001L, 10_002L, 10_003L, 10_004L);
    assertThat(store.after(id, 10_005, 5)).isEmpty();
    assertThat(store.before(id, 1, 5)).isEmpty();
  }

  @Test
  void dmIsUniquePerPair() {
    String a = "a-" + UUID.randomUUID();
    String b = "b-" + UUID.randomUUID();
    String first = store.createDm(a, b, Ids.next());
    String second = store.createDm(b, a, Ids.next());
    assertThat(second).isEqualTo(first);
    assertThat(store.dm(b, a)).contains(first);
  }

  @Test
  void touchUserConvKeepsTheHighestSeq() {
    String id = conv(ConversationKind.CONVERSATION_KIND_GROUP, "tu1", "tu2");
    store.touchUserConv("tu1", id, 5, 5_000);
    store.touchUserConv("tu1", id, 3, 3_000);
    assertThat(store.userConvs("tu1", null, 10)).filteredOn(c -> c.convId().equals(id)).singleElement()
        .satisfies(c -> assertThat(c.lastSeq()).isEqualTo(5));
  }

  /** A removed and re-added member keeps getting its inbox updated (no tombstone shadows later touches). */
  @Test
  void removedThenReAddedMemberIsTouchedAgain() {
    String id = conv(ConversationKind.CONVERSATION_KIND_GROUP, "ra1", "ra2");
    store.touchUserConv("ra2", id, 2, 2_000);
    store.remove(id, "ra2");
    assertThat(store.userConvs("ra2", null, 10)).noneMatch(c -> c.convId().equals(id));
    assertThat(store.member(id, "ra2")).isEmpty();
    store.add(id, new Member("ra2", Role.MEMBER));
    store.touchUserConv("ra2", id, 7, 7_000);
    assertThat(store.userConvs("ra2", null, 10)).filteredOn(c -> c.convId().equals(id)).singleElement()
        .satisfies(c -> assertThat(c.lastSeq()).isEqualTo(7));
  }

  @Test
  void dedupExpiresAfterTtl() {
    String id = conv(ConversationKind.CONVERSATION_KIND_DIRECT, "d1", "d2");
    StoredMessage m = msg(id, 1);
    store.put(m);
    assertThat(store.find(id, m.senderId(), m.clientMsgId())).get()
        .satisfies(f -> assertThat(f.msgId()).isEqualTo(m.msgId()));
    int ttl = session.execute("SELECT TTL(msg_id) FROM chat.send_dedup WHERE conv_id = ? AND sender_id = ? "
        + "AND client_msg_id = ?", id, m.senderId(), m.clientMsgId()).one().getInt(0);
    assertThat(ttl).isGreaterThan(86_000);
  }

  @Test
  void membersAreCachedAndEvicted() {
    String id = Ids.next();
    cache.putMembers(id, List.of(new Member("c1", Role.OWNER), new Member("c2", Role.MEMBER)));
    assertThat(cache.members(id)).contains(List.of(new Member("c1", Role.OWNER), new Member("c2", Role.MEMBER)));
    cache.evictMembers(id);
    assertThat(cache.members(id)).isEmpty();
    StoredMessage m = msg(id, 4);
    cache.putDedup(m);
    assertThat(cache.dedup(id, m.senderId(), m.clientMsgId())).get()
        .satisfies(f -> assertThat(f.convSeq()).isEqualTo(4));
  }

  @Test
  void redisDownIsEmptyNotError() {
    try (RedisChatCache dead = new RedisChatCache(new ChatProperties.Redis("redis://127.0.0.1:1",
        Duration.ofMillis(200), Duration.ofSeconds(60)))) {
      assertThat(dead.members("x")).isEmpty();
      dead.putMembers("x", List.of(new Member("u", Role.MEMBER)));
      assertThat(dead.dedup("x", "u", "c")).isEmpty();
    }
  }

  /** Final review C2: creating an existing conversation again never resets its last_seq fence. */
  @Test
  void createDoesNotOverwriteAnExistingConversation() {
    String id = Ids.next();
    Conversation c = new Conversation(id, ConversationKind.CONVERSATION_KIND_DIRECT, "", "a", 0, false, 2);
    store.create(c, List.of(new Member("a", Role.MEMBER), new Member("b", Role.MEMBER)));
    assertThat(store.reserveSeq(id, 0, 5)).isTrue();
    store.create(c, List.of(new Member("a", Role.MEMBER), new Member("b", Role.MEMBER)));
    assertThat(store.lastSeq(id)).isEqualTo(5);
  }

  /** Final review C2: reads and writes are LOCAL_QUORUM, LWTs LOCAL_SERIAL. */
  @Test
  void consistencyIsQuorum() {
    var profile = session.getContext().getConfig().getDefaultProfile();
    assertThat(profile.getString(com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_QUORUM");
    assertThat(profile.getString(
        com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo("LOCAL_SERIAL");
  }

  /** Final review I5: a member list read before an eviction cannot be cached right after it. */
  @Test
  void evictionBlocksAStaleFill() {
    String conv = Ids.next();
    List<Member> before = List.of(new Member("a", Role.MEMBER), new Member("removed", Role.MEMBER));
    assertThat(cache.evictMembers(conv)).isTrue();
    cache.putMembers(conv, before);   // a reader that loaded the list before the removal
    assertThat(cache.cachedMembers(conv)).isEmpty();
  }
}
