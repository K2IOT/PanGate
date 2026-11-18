package com.pangate.chat.it;

import static com.pangate.chat.support.ChatHarness.T;
import static com.pangate.chat.support.ChatHarness.command;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.Ids;
import com.pangate.chat.support.ChatHarness;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.devtools.client.GatewayClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CH4 end to end on the chat side (spec §0, §3, §4): real gateways, router, Push API and chat-service; a recipient no
 * device took the message from gets a NotifyRequest on c1.notify.requests. notify-service's side (tokens, APNs, FCM)
 * is SendIT's and the smoke's.
 */
class ChatNotifyE2EIT {

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

  static String dm(String a, String b) {
    return h.json(h.call("POST", "/v1/dm/" + b, a, null, Map.of())).get("id").asText();
  }

  static void send(GatewayClient c, String conv, String text, String... mentions) {
    command(c, "chat.send", ChatSend.newBuilder().setConversationId(conv).setClientMsgId(Ids.next()).setText(text)
        .addAllMentionUserIds(List.of(mentions)).build().toByteArray());
  }

  /** The NotifyRequests for {@code user} seen within {@code within}, stopping early once {@code enough} holds. */
  static List<NotifyRequest> requestsFor(String user, Duration within, Predicate<List<NotifyRequest>> enough) {
    List<NotifyRequest> out = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of("bootstrap.servers", h.bootstrap(),
        "group.id", "it-" + UUID.randomUUID(), "auto.offset.reset", "earliest"), new StringDeserializer(),
        new ByteArrayDeserializer())) {
      c.subscribe(List.of("c1.notify.requests"));
      long end = System.nanoTime() + within.toNanos();
      while (System.nanoTime() < end && !enough.test(out)) {
        for (ConsumerRecord<String, byte[]> r : c.poll(Duration.ofMillis(200))) {
          if (("chat:" + user).equals(r.key())) {
            try {
              out.add(NotifyRequest.parseFrom(r.value()));
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
          }
        }
      }
    }
    return out;
  }

  static List<NotifyRequest> awaitOne(String user) {
    List<NotifyRequest> got = requestsFor(user, T, l -> !l.isEmpty());
    assertThat(got).as("a NotifyRequest for " + user).isNotEmpty();
    return got;
  }

  /** Acks the chat.message pushes of a device for a while, so the router counts the message delivered. */
  static void ackFor(GatewayClient c, Duration d) {
    long end = System.nanoTime() + d.toNanos();
    while (System.nanoTime() < end) {
      try {
        Frame f = c.awaitPush(Duration.ofMillis(200));
        if (f.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
          c.ack(f.getMsgId());
        }
      } catch (IllegalStateException timeout) {
        // nothing this time
      }
    }
  }

  @Test
  void onlineRecipientGetsNoNotification() {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    GatewayClient b1 = h.connect(1, b, "d1");
    String d = dm(a, b);
    send(a1, d, "you there?");
    ackFor(b1, Duration.ofSeconds(4));
    assertThat(requestsFor(b, Duration.ofSeconds(4), l -> !l.isEmpty())).isEmpty();
  }

  @Test
  void offlineRecipientGetsTitleBodyAndBadge() {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    String d = dm(a, b);
    send(a1, d, "mai họp lúc 9h");
    NotifyRequest r = awaitOne(b).get(0);
    assertThat(r.getAlert().getTitle()).isEqualTo(a);   // no display name known for this test user
    assertThat(r.getAlert().getBody()).isEqualTo("mai họp lúc 9h");
    assertThat(r.getBadge()).isEqualTo(1);
    assertThat(r.getThreadId()).isEqualTo(d);
  }

  @Test
  void mutedConversationIsSilentUnlessMentioned() {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    String g = h.json(h.call("POST", "/v1/conversations", a, Map.of("kind", "group", "title", "Nhóm",
        "memberIds", List.of(b)), Map.of())).get("id").asText();
    assertThat(h.call("PUT", "/v1/conversations/" + g + "/mute", b, java.util.Collections.singletonMap("until",
        null), Map.of()).statusCode()).isEqualTo(204);
    send(a1, g, "quiet");
    send(a1, g, "@b look", b);
    List<NotifyRequest> got = requestsFor(b, T, l -> !l.isEmpty());
    assertThat(got).extracting(r -> r.getAlert().getBody()).containsExactly(a + ": @b look");
  }

  @Test
  void hidePreviewSendsNoContent() {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    assertThat(h.call("PUT", "/v1/me/privacy", b, Map.of("hidePreview", true), Map.of()).statusCode())
        .isEqualTo(204);
    String d = dm(a, b);
    send(a1, d, "secret");
    NotifyRequest r = awaitOne(b).get(0);
    assertThat(r.getAlert().getTitle()).isEmpty();
    assertThat(r.getAlert().getBody()).isEmpty();
    assertThat(r.getAlert().getLocKey()).isEqualTo("notif_new_message");
  }

  @Test
  void offlineChannelFollowerGetsThePost() {
    String a = user("a");
    String b = user("b");
    GatewayClient a1 = h.connect(0, a, "d1");
    String ch = h.json(h.call("POST", "/v1/conversations", a, Map.of("kind", "channel", "title", "Tin tức",
        "public", true), Map.of())).get("id").asText();
    assertThat(h.call("POST", "/v1/conversations/" + ch + "/join", b, null, Map.of()).statusCode() / 100)
        .isEqualTo(2);
    send(a1, ch, "bài mới");
    NotifyRequest r = awaitOne(b).get(0);
    assertThat(r.getAlert().getTitle()).isEqualTo("Tin tức");
    assertThat(r.getCollapseId()).isEqualTo(ch);
    assertThat(r.getBadge()).isEqualTo(-1);
  }
}
