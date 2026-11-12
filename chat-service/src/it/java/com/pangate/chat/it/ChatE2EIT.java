package com.pangate.chat.it;

import static com.pangate.chat.support.ChatHarness.T;
import static com.pangate.chat.support.ChatHarness.chatSend;
import static com.pangate.chat.support.ChatHarness.command;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.pangate.chat.core.Ids;
import com.pangate.chat.support.ChatHarness;
import com.pangate.contracts.v1.ChannelOpen;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.devtools.client.GatewayClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Chat end to end (chat spec §9): WebSocket and HTTP in, Push API → router → gateways out. */
class ChatE2EIT {

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
    return name + "-" + UUID.randomUUID().toString().substring(0, 6);
  }

  /** The next chat.message PUSH, acked. */
  static ChatMessage next(GatewayClient c) throws Exception {
    while (true) {
      Frame f = c.awaitPush(T);
      if (f.getClass_() == com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_RELIABLE) {
        c.ack(f.getMsgId());
      }
      if ("chat.message".equals(f.getHeadersMap().get("type"))) {
        return ChatMessage.parseFrom(f.getPayload());
      }
    }
  }

  /** The next chat.message PUSH within {@code wait}, or null. */
  static ChatMessage poll(GatewayClient c, Duration wait) throws Exception {
    long deadline = System.nanoTime() + wait.toNanos();
    while (System.nanoTime() < deadline) {
      Frame f;
      try {
        f = c.awaitPush(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
      } catch (IllegalStateException timeout) {
        return null;
      }
      if (f.getClass_() == com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_RELIABLE) {
        c.ack(f.getMsgId());
      }
      if ("chat.message".equals(f.getHeadersMap().get("type"))) {
        return ChatMessage.parseFrom(f.getPayload());
      }
    }
    return null;
  }

  static List<Long> seqs(GatewayClient c, int n) throws Exception {
    List<Long> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(next(c).getConvSeq());
    }
    return out;
  }

  String group(String owner, String... members) {
    HttpResponse<String> r = h.call("POST", "/v1/conversations", owner, Map.of("kind", "group", "title", "g",
        "memberIds", List.of(members)), Map.of());
    assertThat(r.statusCode()).as(r.body()).isEqualTo(201);
    return h.json(r).get("id").asText();
  }

  JsonNode httpSend(String user, String conv, String text, String key) {
    HttpResponse<String> r = h.call("POST", "/v1/conversations/" + conv + "/messages", user, Map.of("text", text),
        Map.of("Idempotency-Key", key));
    assertThat(r.statusCode()).as(r.body()).isIn(201, 202);
    return h.json(r);
  }

  @Test
  void directMessageArrivesInConvSeqOrder() throws Exception {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    GatewayClient a2 = h.connect(1, a, "d2");
    GatewayClient b1 = h.connect(1, b, "d1");
    String dm = h.json(h.call("POST", "/v1/dm/" + b, a, null, Map.of())).get("id").asText();
    List<String> texts = List.of("one", "two", "three");
    for (String t : texts) {
      Frame ack = command(a1, "chat.send", chatSend(dm, t).toByteArray());
      assertThat(ack.getType()).isEqualTo(FrameType.FRAME_TYPE_UPSTREAM_ACK);
    }
    List<ChatMessage> got = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      got.add(next(b1));
    }
    assertThat(got).extracting(ChatMessage::getConvSeq).containsExactly(1L, 2L, 3L);
    assertThat(got).extracting(ChatMessage::getText).containsExactlyElementsOf(texts);
    assertThat(got).extracting(ChatMessage::getSenderId).containsOnly(a);
    assertThat(seqs(a2, 3)).as("the sender's other device").containsExactly(1L, 2L, 3L);
    assertThat(seqs(a1, 3)).as("the sending device replaces its draft").containsExactly(1L, 2L, 3L);
  }

  @Test
  void groupOfThreeGetsEveryMessage() throws Exception {
    String a = user("a");
    String b = user("b");
    String c = user("c");
    GatewayClient ca = h.connect(0, a, "d1");
    GatewayClient cb = h.connect(1, b, "d1");
    GatewayClient cc = h.connect(0, c, "d1");
    String g = group(a, b, c);
    command(ca, "chat.send", chatSend(g, "from a").toByteArray());
    next(ca);
    httpSend(b, g, "from b", Ids.next());
    for (GatewayClient x : List.of(cb, cc)) {
      List<ChatMessage> two = List.of(next(x), next(x));
      assertThat(two).extracting(ChatMessage::getText).containsExactly("from a", "from b");
    }
    assertThat(next(ca).getText()).isEqualTo("from b");
  }

  @Test
  void channelOpenThenBroadcast() throws Exception {
    String owner = user("own");
    String fan = user("fan");
    String ch = h.json(h.call("POST", "/v1/conversations", owner, Map.of("kind", "channel", "title", "news",
        "public", true), Map.of())).get("id").asText();
    GatewayClient f = h.connect(0, fan, "d1");
    httpSend(owner, ch, "before open", Ids.next());
    assertThatThrownBy(() -> f.awaitPush(Duration.ofSeconds(3))).as("not subscribed yet")
        .isInstanceOf(IllegalStateException.class);
    assertThat(command(f, "chat.channel.open", ChannelOpen.newBuilder().setConversationId(ch).build()
        .toByteArray()).getType()).isEqualTo(FrameType.FRAME_TYPE_UPSTREAM_ACK);
    // the subscription goes Push API → Kafka → router → gateway; post until it is in place
    ChatMessage got = null;
    long deadline = System.nanoTime() + T.toNanos();
    while (got == null && System.nanoTime() < deadline) {
      httpSend(owner, ch, "news", Ids.next());
      try {
        got = next(f);
      } catch (IllegalStateException e) {
        // not yet
      }
    }
    assertThat(got).isNotNull();
    assertThat(got.getText()).isEqualTo("news");
    assertThat(got.getConvSeq()).isGreaterThan(1);
  }

  @Test
  void reconnectAndSyncGetsWhatWasMissed() throws Exception {
    String a = user("a");
    String b = user("b");
    String dm = h.json(h.call("POST", "/v1/dm/" + b, a, null, Map.of())).get("id").asText();
    GatewayClient b1 = h.connect(0, b, "d1");
    httpSend(a, dm, "seen", Ids.next());
    assertThat(next(b1).getConvSeq()).isEqualTo(1);
    b1.close();
    for (int i = 0; i < 5; i++) {
      httpSend(a, dm, "missed " + i, Ids.next());
    }
    h.connect(1, b, "d1");
    JsonNode sync = h.json(h.call("POST", "/v1/sync", b, Map.of("conversations", List.of(Map.of("id", dm,
        "lastSeq", 1))), Map.of()));
    JsonNode msgs = sync.get("conversations").get(0).get("messages");
    assertThat(msgs).hasSize(5);
    List<Long> got = new ArrayList<>();
    msgs.forEach(m -> got.add(m.get("convSeq").asLong()));
    assertThat(got).containsExactly(2L, 3L, 4L, 5L, 6L);
  }

  @Test
  void resendSameClientMsgIdIsOneMessage() throws Exception {
    String a = user("a");
    String b = user("b");
    String dm = h.json(h.call("POST", "/v1/dm/" + b, a, null, Map.of())).get("id").asText();
    GatewayClient ca = h.connect(0, a, "d1");
    GatewayClient cb = h.connect(1, b, "d1");
    ChatSend s = chatSend(dm, "once");
    command(ca, "chat.send", s.toByteArray());
    command(ca, "chat.send", s.toByteArray());
    JsonNode viaHttp = httpSend(a, dm, "once", s.getClientMsgId());
    ChatMessage first = next(cb);
    assertThat(first.getConvSeq()).isEqualTo(1);
    // repeats may be re-emitted (spec §6: clients dedup by msg_id); never a second message
    try {
      while (true) {
        assertThat(next(cb).getMsgId()).isEqualTo(first.getMsgId());
      }
    } catch (IllegalStateException timeout) {
      // no more pushes
    }
    if (viaHttp.has("msgId")) {
      assertThat(viaHttp.get("msgId").asText()).isEqualTo(first.getMsgId());
    }
    assertThat(h.json(h.call("GET", "/v1/conversations/" + dm, a, null, Map.of())).get("lastSeq").asLong())
        .isEqualTo(1);
  }

  @Test
  void rejectedSendIsPushedBackToSender() throws Exception {
    String a = user("a");
    String stranger = user("s");
    String g = group(a, user("b"));
    GatewayClient s = h.connect(0, stranger, "d1");
    ChatSend send = chatSend(g, "let me in");
    command(s, "chat.send", send.toByteArray());
    Frame f = s.awaitPush(T);
    assertThat(f.getHeadersMap()).containsEntry("type", "chat.rejected");
    ChatRejected r = ChatRejected.parseFrom(f.getPayload());
    assertThat(r.getReason()).isEqualTo("not_member");
    assertThat(r.getClientMsgId()).isEqualTo(send.getClientMsgId());
    assertThat(r.getConversationId()).isEqualTo(g);
  }
  /**
   * A member removed from a private channel stops getting its messages without reconnecting: the removal revokes the
   * open subscriptions server side (Push API → router → gateway), while the members who stay keep receiving.
   */
  @Test
  void removedMemberOfPrivateChannelStopsReceiving() throws Exception {
    String owner = user("own");
    String gone = user("gone");
    String stays = user("stays");
    HttpResponse<String> created = h.call("POST", "/v1/conversations", owner, Map.of("kind", "channel", "title",
        "private", "public", false, "memberIds", List.of(gone, stays)), Map.of());
    assertThat(created.statusCode()).as(created.body()).isEqualTo(201);
    String ch = h.json(created).get("id").asText();
    GatewayClient g = h.connect(0, gone, "d1");
    GatewayClient s = h.connect(1, stays, "d1");
    for (GatewayClient c : List.of(g, s)) {
      assertThat(command(c, "chat.channel.open", ChannelOpen.newBuilder().setConversationId(ch).build()
          .toByteArray()).getType()).isEqualTo(FrameType.FRAME_TYPE_UPSTREAM_ACK);
    }
    // both subscriptions in place: post until each member got a message
    boolean gotG = false;
    boolean gotS = false;
    long deadline = System.nanoTime() + T.toNanos();
    while (!(gotG && gotS) && System.nanoTime() < deadline) {
      httpSend(owner, ch, "warm-up", Ids.next());
      gotG |= poll(g, Duration.ofSeconds(2)) != null;
      gotS |= poll(s, Duration.ofSeconds(2)) != null;
    }
    assertThat(gotG && gotS).as("both members subscribed").isTrue();

    assertThat(h.call("DELETE", "/v1/conversations/" + ch + "/members/" + gone, owner, null, Map.of())
        .statusCode()).isEqualTo(204);
    // the revoke travels like the subscribe did: post until a message reaches the member who stays and not the other
    boolean cut = false;
    deadline = System.nanoTime() + T.toNanos();
    for (int i = 0; !cut && System.nanoTime() < deadline; i++) {
      String text = "after removal " + i;
      httpSend(owner, ch, text, Ids.next());
      ChatMessage m;
      do {
        m = poll(s, T);
        assertThat(m).as("the member who stays still receives").isNotNull();
      } while (!m.getText().equals(text));
      boolean leaked = false;
      for (ChatMessage x = poll(g, Duration.ofSeconds(2)); x != null; x = poll(g, Duration.ofSeconds(2))) {
        leaked |= x.getText().equals(text);
      }
      cut = !leaked;
    }
    assertThat(cut).as("the removed member stops receiving the private channel").isTrue();

    // and cannot subscribe again
    command(g, "chat.channel.open", ChannelOpen.newBuilder().setConversationId(ch).build().toByteArray());
    Frame f;
    do {
      f = g.awaitPush(T);
    } while (!"chat.rejected".equals(f.getHeadersMap().get("type")));
    assertThat(ChatRejected.parseFrom(f.getPayload()).getReason()).isEqualTo("not_member");
  }
}
