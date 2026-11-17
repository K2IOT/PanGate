package com.pangate.chat.it;

import static com.pangate.chat.support.ChatHarness.T;
import static com.pangate.chat.support.ChatHarness.chatSend;
import static com.pangate.chat.support.ChatHarness.command;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.pangate.chat.support.ChatHarness;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ChatTyping;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.PresenceUpdate;
import com.pangate.contracts.v1.PresenceWatch;
import com.pangate.contracts.v1.ReceiptUpdate;
import com.pangate.contracts.v1.TypingUpdate;
import com.pangate.devtools.client.GatewayClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** CH3 end to end (spec §0 success criteria): real gateways, router, Push API and chat-service. */
class ChatStateE2EIT {

  @TempDir static Path devDir;
  static ChatHarness h;

  @BeforeAll
  static void boot() throws Exception {
    h = new ChatHarness(devDir).start();
  }

  @AfterAll
  static void shutdown() {
    if (h != null) {
      h.close();
    }
  }

  @AfterEach
  void closeClients() {
    h.closeClients();
  }

  static String user(String name) {
    String id = name + "-" + UUID.randomUUID().toString().substring(0, 6);
    com.pangate.chat.support.KnownUsers.add(h.bootstrap(), h.chat, id);
    return id;
  }

  /** The next PUSH with {@code headers.type == type} within {@code wait} (others are acked and skipped), or null. */
  static Frame poll(GatewayClient c, String type, Duration wait) {
    long deadline = System.nanoTime() + wait.toNanos();
    while (System.nanoTime() < deadline) {
      Frame f;
      try {
        f = c.awaitPush(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
      } catch (IllegalStateException timeout) {
        return null;
      }
      if (f.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
        c.ack(f.getMsgId());
      }
      if (type.equals(f.getHeadersMap().get("type"))) {
        return f;
      }
    }
    return null;
  }

  static Frame next(GatewayClient c, String type) {
    Frame f = poll(c, type, T);
    assertThat(f).as("a " + type + " push").isNotNull();
    return f;
  }

  static String dm(String a, String b) {
    return h.json(h.call("POST", "/v1/dm/" + b, a, null, Map.of())).get("id").asText();
  }

  /** Presence counts a DM only once the watched user sent a message there (owner decision 2026-11-17). */
  static void talk(GatewayClient c, String conv) {
    command(c, "chat.send", chatSend(conv, "hello").toByteArray());
    next(c, "chat.message");
  }

  static void mark(GatewayClient c, String op, String conv, long seq) {
    command(c, op, ChatMark.newBuilder().setConversationId(conv).setUpToSeq(seq).build().toByteArray());
  }

  static void watch(GatewayClient c, String conv, String... users) {
    PresenceWatch.Builder w = PresenceWatch.newBuilder();
    for (String u : users) {
      w.addEntries(PresenceWatch.Entry.newBuilder().setUserId(u).setConversationId(conv));
    }
    command(c, ChatTopics.OP_WATCH, w.build().toByteArray());
  }

  /** Waits for a receipt update carrying user's mark at (at least) the given values. */
  static ReceiptUpdate.Mark awaitMark(GatewayClient c, String user, long delivered, long read) throws Exception {
    long deadline = System.nanoTime() + T.toNanos();
    while (System.nanoTime() < deadline) {
      Frame f = next(c, ChatTopics.TYPE_RECEIPTS);
      for (ReceiptUpdate.Mark m : ReceiptUpdate.parseFrom(f.getPayload()).getMarksList()) {
        if (m.getUserId().equals(user) && m.getDeliveredSeq() >= delivered && m.getReadSeq() >= read) {
          return m;
        }
      }
    }
    throw new AssertionError("no receipt of " + user);
  }

  @Test
  void dmDeliveredThenReadReachSenderWithinASecond() throws Exception {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    GatewayClient b1 = h.connect(1, b, "d1");
    String d = dm(a, b);
    command(a1, "chat.send", chatSend(d, "hi").toByteArray());
    ChatMessage m = ChatMessage.parseFrom(next(b1, "chat.message").getPayload());
    long t0 = System.nanoTime();
    mark(b1, ChatTopics.OP_DELIVERED, d, m.getConvSeq());
    ReceiptUpdate.Mark delivered = awaitMark(a1, b, 1, 0);
    assertThat(delivered.getReadSeq()).isZero();
    assertThat((System.nanoTime() - t0) / 1_000_000).as("ms to Đã nhận").isLessThan(1_000);
    t0 = System.nanoTime();
    mark(b1, ChatTopics.OP_READ, d, m.getConvSeq());
    awaitMark(a1, b, 1, 1);
    assertThat((System.nanoTime() - t0) / 1_000_000).as("ms to Đã xem").isLessThan(1_000);
  }

  @Test
  void group3ListsReaders() throws Exception {
    String a = user("a");
    String b = user("b");
    String c = user("c");
    GatewayClient a1 = h.connect(0, a, "d1");
    HttpResponse<String> r = h.call("POST", "/v1/conversations", a, Map.of("kind", "group", "title", "g",
        "memberIds", List.of(b, c)), Map.of());
    String g = h.json(r).get("id").asText();
    command(a1, "chat.send", chatSend(g, "hello").toByteArray());
    next(a1, "chat.message");
    for (String u : List.of(b, c)) {
      assertThat(h.call("POST", "/v1/conversations/" + g + "/read", u, Map.of("upToSeq", 1), Map.of()).statusCode())
          .isEqualTo(204);
    }
    Set<String> readers = new HashSet<>();
    long deadline = System.nanoTime() + T.toNanos();
    while (readers.size() < 2 && System.nanoTime() < deadline) {
      for (ReceiptUpdate.Mark m : ReceiptUpdate.parseFrom(next(a1, ChatTopics.TYPE_RECEIPTS).getPayload())
          .getMarksList()) {
        if (m.getReadSeq() >= 1) {
          readers.add(m.getUserId());
        }
      }
    }
    assertThat(readers).containsExactlyInAnyOrder(b, c);
  }

  @Test
  void readOnDevice1ClearsUnreadOnDevice2() throws Exception {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    GatewayClient b1 = h.connect(0, b, "d1");
    GatewayClient b2 = h.connect(1, b, "d2");
    String d = dm(a, b);
    command(a1, "chat.send", chatSend(d, "x").toByteArray());
    next(b1, "chat.message");
    next(b2, "chat.message");
    mark(b1, ChatTopics.OP_READ, d, 1);
    ReceiptUpdate own = ReceiptUpdate.parseFrom(next(b2, ChatTopics.TYPE_RECEIPTS).getPayload());
    assertThat(own.getOwnReadSeq()).isEqualTo(1);
    // a device that was away gets it from /v1/state (spec §7.2)
    JsonNode state = h.json(h.call("GET", "/v1/state", b, null, Map.of()));
    for (JsonNode c : state.get("conversations")) {
      if (c.get("id").asText().equals(d)) {
        assertThat(c.get("unread").asInt()).isZero();
      }
    }
  }

  @Test
  void typingReachesPeerAndExpires() throws Exception {
    String a = user("a");
    String b = user("b");
    String outsider = user("o");
    GatewayClient a1 = h.connect(0, a, "d1");
    GatewayClient b1 = h.connect(1, b, "d1");
    GatewayClient o1 = h.connect(1, outsider, "d1");
    String d = dm(a, b);
    command(a1, ChatTopics.OP_TYPING, ChatTyping.newBuilder().setConversationId(d).build().toByteArray());
    TypingUpdate t = TypingUpdate.parseFrom(next(b1, ChatTopics.TYPE_TYPING).getPayload());
    assertThat(t.getUserId()).isEqualTo(a);
    assertThat(t.getTtlMs()).isEqualTo(6000);
    assertThat(poll(o1, ChatTopics.TYPE_TYPING, Duration.ofSeconds(2))).isNull();
    assertThat(poll(a1, ChatTopics.TYPE_TYPING, Duration.ofMillis(500))).as("not to the typer").isNull();
  }

  @Test
  void disconnectShowsOfflineAfterGraceToWatcher() throws Exception {
    String a = user("a");
    String b = user("b");
    GatewayClient b1 = h.connect(1, b, "d1");
    GatewayClient a1 = h.connect(0, a, "d1");
    String d = dm(a, b);
    talk(b1, d);
    Thread.sleep(1_000);   // b's CONNECTED reached chat-service
    watch(a1, d, b);
    PresenceUpdate snap = PresenceUpdate.parseFrom(next(a1, ChatTopics.TYPE_PRESENCE).getPayload());
    assertThat(snap.getStates(0).getOnline()).isTrue();
    long t0 = System.nanoTime();
    b1.close();
    assertThat(poll(a1, ChatTopics.TYPE_PRESENCE, Duration.ofMillis(3_500))).as("nothing inside the grace").isNull();
    PresenceUpdate off = PresenceUpdate.parseFrom(next(a1, ChatTopics.TYPE_PRESENCE).getPayload());
    assertThat(off.getStates(0).getOnline()).isFalse();
    assertThat(off.getStates(0).getLastSeenMs()).isPositive();
    assertThat((System.nanoTime() - t0) / 1_000_000).isGreaterThanOrEqualTo(4_500);
  }

  @Test
  void flapUnder5sShowsNothing() throws Exception {
    String a = user("a");
    String b = user("b");
    GatewayClient b1 = h.connect(1, b, "d1");
    GatewayClient a1 = h.connect(0, a, "d1");
    String d = dm(a, b);
    talk(b1, d);
    Thread.sleep(1_000);
    watch(a1, d, b);
    next(a1, ChatTopics.TYPE_PRESENCE);
    b1.close();
    Thread.sleep(1_500);
    h.connect(0, b, "d1");
    assertThat(poll(a1, ChatTopics.TYPE_PRESENCE, Duration.ofSeconds(8))).isNull();
  }

  @Test
  void strangerCannotWatch() throws Exception {
    String a = user("a");
    String b = user("b");
    String s = user("s");
    h.connect(1, b, "d1");
    GatewayClient s1 = h.connect(0, s, "d1");
    String d = dm(a, b);
    watch(s1, d, b);
    assertThat(poll(s1, ChatTopics.TYPE_PRESENCE, Duration.ofSeconds(3))).isNull();
    HttpResponse<String> r = h.call("GET", "/v1/presence?ids=" + b + "&conversationId=" + d, s, null, Map.of());
    assertThat(h.json(r).get("states")).isEmpty();
  }

  @Test
  void hidePresenceIsReciprocal() throws Exception {
    String a = user("a");
    String b = user("b");
    GatewayClient b1 = h.connect(1, b, "d1");
    GatewayClient a1 = h.connect(0, a, "d1");
    String d = dm(a, b);
    talk(b1, d);
    assertThat(h.call("PUT", "/v1/me/privacy", a, Map.of("hidePresence", true), Map.of()).statusCode())
        .isEqualTo(204);
    Thread.sleep(1_000);
    watch(a1, d, b);
    PresenceUpdate.State s = PresenceUpdate.parseFrom(next(a1, ChatTopics.TYPE_PRESENCE).getPayload()).getStates(0);
    assertThat(s.getHidden()).isTrue();
    assertThat(s.getOnline()).isFalse();
  }

  /** Spec §7: a client that lost pushes sees the gap, fills it with /v1/sync, and ends with no duplicate. */
  @Test
  void gapIsFilledBySyncWithoutDuplicates() throws Exception {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    String d = dm(a, b);
    for (int i = 1; i <= 3; i++) {
      command(a1, "chat.send", chatSend(d, "m" + i).toByteArray());
    }
    for (int i = 1; i <= 3; i++) {
      next(a1, "chat.message");
    }
    GatewayClient b1 = h.connect(1, b, "d1");   // b missed 1..3
    Thread.sleep(3_000);   // the router caches registry lookups for 2 s, an empty one too (router.yml): outwait it
    command(a1, "chat.send", chatSend(d, "m4").toByteArray());
    ChatMessage m4 = ChatMessage.parseFrom(next(b1, "chat.message").getPayload());
    long synced = 0;
    assertThat(m4.getConvSeq()).as("a gap: syncedSeq + 1 < convSeq").isGreaterThan(synced + 1);
    JsonNode sync = h.json(h.call("POST", "/v1/sync", b, Map.of("conversations", List.of(Map.of("id", d,
        "lastSeq", synced))), Map.of()));
    Set<String> ids = new HashSet<>();
    List<Long> seqs = new java.util.ArrayList<>();
    for (JsonNode m : sync.get("conversations").get(0).get("messages")) {
      ids.add(m.get("msgId").asText());
      seqs.add(m.get("convSeq").asLong());
    }
    ids.add(m4.getMsgId());   // dedup by msg_id (chat spec §6)
    assertThat(seqs).containsExactly(1L, 2L, 3L, 4L);
    assertThat(ids).hasSize(4);
  }

  /** Owner decision 2026-11-17: an empty DM, or a group the viewer added the user to, does not reveal presence. */
  @Test
  void emptyDmOrSelfAddedGroupRevealsNothing() throws Exception {
    String a = user("a");
    String b = user("b");
    h.connect(1, b, "d1");
    GatewayClient a1 = h.connect(0, a, "d1");
    String d = dm(a, b);
    HttpResponse<String> r = h.call("POST", "/v1/conversations", a, Map.of("kind", "group", "title", "g",
        "memberIds", List.of(b)), Map.of());
    String g = h.json(r).get("id").asText();
    watch(a1, d, b);
    assertThat(poll(a1, ChatTopics.TYPE_PRESENCE, Duration.ofSeconds(2))).isNull();
    watch(a1, g, b);
    assertThat(poll(a1, ChatTopics.TYPE_PRESENCE, Duration.ofSeconds(2))).isNull();
    assertThat(h.json(h.call("GET", "/v1/presence?ids=" + b + "&conversationId=" + g, a, null, Map.of()))
        .get("states")).isEmpty();
  }
}
