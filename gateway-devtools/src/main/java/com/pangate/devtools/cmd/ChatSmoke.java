package com.pangate.devtools.cmd;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.pangate.contracts.v1.ChannelOpen;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.devtools.auth.AuthClient;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * The chat steps of {@code smoke --chat} (chat spec §9): CHAT 1-1 (WebSocket send → the other user's PUSH), CHAT
 * group (HTTP send → both other members), CHAT channel ({@code chat.channel.open} → the owner's post arrives). Users
 * are fresh per run; tokens come from auth-service for the {@code chat-backend} client.
 */
final class ChatSmoke {

  static final Duration T = Duration.ofSeconds(10);
  static final String CLIENT = "chat-backend";
  static final String ULID = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

  private final URI chat;
  private final URI ws;
  private final ClientTls tls;
  private final AuthClient auth;
  private final HttpClient http = HttpClient.newHttpClient();
  private final List<GatewayClient> open = new ArrayList<>();

  ChatSmoke(URI chat, URI ws, ClientTls tls, AuthClient auth) {
    this.chat = chat;
    this.ws = ws;
    this.tls = tls;
    this.auth = auth;
  }

  static String user(String name) {
    return "smoke-" + name + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  static String ulid() {
    StringBuilder b = new StringBuilder();
    long t = System.currentTimeMillis();
    char[] time = new char[10];
    for (int i = 9; i >= 0; i--) {
      time[i] = ULID.charAt((int) (t & 31));
      t >>>= 5;
    }
    b.append(time);
    java.util.concurrent.ThreadLocalRandom r = java.util.concurrent.ThreadLocalRandom.current();
    for (int i = 0; i < 16; i++) {
      b.append(ULID.charAt(r.nextInt(32)));
    }
    return b.toString();
  }

  String token(String user) {
    return auth.connectToken("chat:" + user, "smoke", List.of());
  }

  GatewayClient connect(String user) {
    GatewayClient c = GatewayClient.connect(ws, tls, T);
    open.add(c);
    c.hello(Hellos.jwt(token(user), "smoke"), T);
    return c;
  }

  Map<String, Object> call(String method, String path, String user, String body, String key) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(chat.resolve(path)).timeout(T)
        .header("Authorization", "Bearer " + token(user));
    if (key != null) {
      b.header("Idempotency-Key", key);
    }
    b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    if (body != null) {
      b.header("Content-Type", "application/json");
    }
    HttpResponse<String> res = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() / 100 != 2) {
      throw new IllegalStateException(method + " " + path + " answered " + res.statusCode() + ": " + res.body());
    }
    return res.body().isEmpty() ? Map.of() : JSONObjectUtils.parse(res.body());
  }

  static void command(GatewayClient c, String op, byte[] payload) {
    String id = UUID.randomUUID().toString();
    c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId(id).putHeaders("type", "cmd")
        .putHeaders("op", op).setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build());
    Frame f = c.await(FrameType.FRAME_TYPE_UPSTREAM_ACK, T);
    if (!id.equals(f.getMsgId())) {
      throw new IllegalStateException("UPSTREAM_ACK for another frame");
    }
  }

  /**
   * The next chat.message PUSH of {@code convId} (acked when RELIABLE); messages of other conversations are skipped
   * (a sender's own devices get its messages too).
   */
  static ChatMessage next(GatewayClient c, String convId, Duration within) throws Exception {
    long deadline = System.nanoTime() + within.toNanos();
    while (true) {
      Frame f = c.awaitPush(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
      if (f.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
        c.ack(f.getMsgId());
      }
      if ("chat.message".equals(f.getHeadersMap().get("type"))) {
        ChatMessage m = ChatMessage.parseFrom(f.getPayload());
        if (m.getConversationId().equals(convId)) {
          return m;
        }
      }
    }
  }

  void run(Consumer<String> step) throws Exception {
    try {
      step.accept("CHAT 1-1");
      String a = user("a");
      String b = user("b");
      GatewayClient ca = connect(a);
      GatewayClient cb = connect(b);
      String dm = call("POST", "/v1/dm/" + b, a, null, null).get("id").toString();
      long t0 = System.nanoTime();
      command(ca, "chat.send", ChatSend.newBuilder().setConversationId(dm).setClientMsgId(ulid()).setText("hi")
          .build().toByteArray());
      ChatMessage m = next(cb, dm, T);
      if (m.getConvSeq() != 1 || !m.getSenderId().equals(a)) {
        throw new IllegalStateException("unexpected message " + m);
      }
      System.out.printf("CHAT 1-1: send -> PUSH in %.1f ms%n", (System.nanoTime() - t0) / 1e6);

      step.accept("CHAT group");
      String c = user("c");
      GatewayClient cc = connect(c);
      String g = call("POST", "/v1/conversations", a, "{\"kind\":\"group\",\"title\":\"smoke\",\"memberIds\":[\""
          + b + "\",\"" + c + "\"]}", null).get("id").toString();
      t0 = System.nanoTime();
      Map<String, Object> sent = call("POST", "/v1/conversations/" + g + "/messages", b, "{\"text\":\"group\"}",
          ulid());
      for (GatewayClient x : List.of(ca, cc)) {
        ChatMessage gm = next(x, g, T);
        if (!gm.getConversationId().equals(g) || !"group".equals(gm.getText())) {
          throw new IllegalStateException("unexpected message " + gm);
        }
      }
      System.out.printf("CHAT group: HTTP %s -> 2 members in %.1f ms%n", sent.containsKey("msgId") ? "201" : "202",
          (System.nanoTime() - t0) / 1e6);

      step.accept("CHAT channel");
      String ch = call("POST", "/v1/conversations", a, "{\"kind\":\"channel\",\"title\":\"smoke\",\"public\":true}",
          null).get("id").toString();
      command(cb, "chat.channel.open", ChannelOpen.newBuilder().setConversationId(ch).build().toByteArray());
      ChatMessage cm = null;
      long deadline = System.nanoTime() + T.toNanos();
      while (cm == null && System.nanoTime() < deadline) {   // the subscribe reaches the gateway asynchronously
        call("POST", "/v1/conversations/" + ch + "/messages", a, "{\"text\":\"news\"}", ulid());
        try {
          cm = next(cb, ch, Duration.ofSeconds(1));
        } catch (IllegalStateException notYet) {
          // post again
        }
      }
      if (cm == null || !cm.getConversationId().equals(ch)) {
        throw new IllegalStateException("no channel message within " + T);
      }
      System.out.println("CHAT channel: open -> broadcast seq " + cm.getConvSeq());
    } finally {
      for (GatewayClient x : open) {
        try {
          x.close();
        } catch (RuntimeException ignored) {
          // closed
        }
      }
    }
  }
}
