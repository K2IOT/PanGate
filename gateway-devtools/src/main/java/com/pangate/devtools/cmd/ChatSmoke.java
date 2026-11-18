package com.pangate.devtools.cmd;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.pangate.contracts.v1.ChannelOpen;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
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
 * are fresh accounts registered in account-service per run (chat-service only lets known users into conversations,
 * account spec §8); their connect tokens come from refresh token exchanges.
 */
final class ChatSmoke {

  static final List<String> STEPS = List.of("CHAT 1-1", "CHAT group", "CHAT channel", "CHAT receipts",
      "CHAT typing", "CHAT presence", "CHAT notify");

  static final Duration T = Duration.ofSeconds(10);
  static final String ULID = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

  private final URI chat;
  private final URI ws;
  private final ClientTls tls;
  private final AccountApi accounts;
  private final Map<String, AccountApi.Account> byUserId = new java.util.concurrent.ConcurrentHashMap<>();
  private final HttpClient http = HttpClient.newHttpClient();
  private final List<GatewayClient> open = new ArrayList<>();

  private final URI notify;
  private final URI fakeApns;

  ChatSmoke(URI chat, URI ws, ClientTls tls, AccountApi accounts) {
    this(chat, ws, tls, accounts, null, null);
  }

  /** {@code notify}, {@code fakeApns}: the CHAT notify step (CH4 spec §7); both null skips it. */
  ChatSmoke(URI chat, URI ws, ClientTls tls, AccountApi accounts, URI notify, URI fakeApns) {
    this.notify = notify;
    this.fakeApns = fakeApns;
    this.chat = chat;
    this.ws = ws;
    this.tls = tls;
    this.accounts = accounts;
  }

  /** A fresh, valid username (account spec §4.1). */
  static String user(String name) {
    return "smoke_" + name + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  /** Registers a fresh account; returns its userId. */
  String newUser(String name) {
    AccountApi.Account a = accounts.register(user(name));
    byUserId.put(a.userId(), a);
    return a.userId();
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

  String token(String userId) {
    return accounts.connectToken(byUserId.get(userId).refreshToken());
  }

  GatewayClient connect(String user) {
    GatewayClient c = GatewayClient.connect(ws, tls, T);
    open.add(c);
    c.hello(Hellos.jwt(token(user), byUserId.get(user).deviceId()), T);
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

  /**
   * Like {@link #call}, but a {@code 404 user_not_found} is retried for up to {@link #T}: a just-registered account
   * reaches chat-service through account.users a few hundred ms later (account spec §8).
   */
  Map<String, Object> callNewUsers(String method, String path, String user, String body) throws Exception {
    long deadline = System.nanoTime() + T.toNanos();
    while (true) {
      try {
        return call(method, path, user, body, null);
      } catch (IllegalStateException e) {
        if (!e.getMessage().contains("user_not_found") || System.nanoTime() > deadline) {
          throw e;
        }
        Thread.sleep(200);
      }
    }
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

  /** The next PUSH with {@code headers.type == type} within {@code within}; others are acked and skipped. */
  static Frame nextOfType(GatewayClient c, String type, Duration within) {
    long deadline = System.nanoTime() + within.toNanos();
    while (true) {
      Frame f = c.awaitPush(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
      if (f.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
        c.ack(f.getMsgId());
      }
      if (type.equals(f.getHeadersMap().get("type"))) {
        return f;
      }
    }
  }

  void run(Consumer<String> step) throws Exception {
    try {
      step.accept(STEPS.get(0));
      String a = newUser("a");
      String b = newUser("b");
      String c = newUser("c");
      GatewayClient ca = connect(a);
      GatewayClient cb = connect(b);
      String dm = callNewUsers("POST", "/v1/dm/" + b, a, null).get("id").toString();
      long t0 = System.nanoTime();
      command(ca, "chat.send", ChatSend.newBuilder().setConversationId(dm).setClientMsgId(ulid()).setText("hi")
          .build().toByteArray());
      ChatMessage m = next(cb, dm, T);
      if (m.getConvSeq() != 1 || !m.getSenderId().equals(a)) {
        throw new IllegalStateException("unexpected message " + m);
      }
      System.out.printf("CHAT 1-1: send -> PUSH in %.1f ms%n", (System.nanoTime() - t0) / 1e6);

      step.accept(STEPS.get(1));
      GatewayClient cc = connect(c);
      String g = callNewUsers("POST", "/v1/conversations", a, "{\"kind\":\"group\",\"title\":\"smoke\",\"memberIds\":[\""
          + b + "\",\"" + c + "\"]}").get("id").toString();
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

      step.accept(STEPS.get(2));
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

      // CH3 spec §10: conversation state
      step.accept(STEPS.get(3));
      t0 = System.nanoTime();
      command(cb, "chat.read", com.pangate.contracts.v1.ChatMark.newBuilder().setConversationId(dm).setUpToSeq(1)
          .build().toByteArray());
      boolean seen = false;
      while (!seen) {
        var u = com.pangate.contracts.v1.ReceiptUpdate.parseFrom(nextOfType(ca, "chat.receipts", T).getPayload());
        seen = u.getMarksList().stream().anyMatch(x -> x.getUserId().equals(b) && x.getReadSeq() >= 1);
      }
      System.out.printf("CHAT receipts: read -> sender sees it in %.1f ms%n", (System.nanoTime() - t0) / 1e6);

      step.accept(STEPS.get(4));
      t0 = System.nanoTime();
      command(cb, "chat.typing", com.pangate.contracts.v1.ChatTyping.newBuilder().setConversationId(dm).build()
          .toByteArray());
      var typing = com.pangate.contracts.v1.TypingUpdate.parseFrom(nextOfType(ca, "chat.typing", T).getPayload());
      if (!typing.getUserId().equals(b)) {
        throw new IllegalStateException("unexpected typing " + typing);
      }
      System.out.printf("CHAT typing: typing -> peer in %.1f ms%n", (System.nanoTime() - t0) / 1e6);

      step.accept(STEPS.get(5));
      // presence counts a DM once the watched user sent a message there (owner decision 2026-11-17)
      command(cb, "chat.send", ChatSend.newBuilder().setConversationId(dm).setClientMsgId(ulid()).setText("hey")
          .build().toByteArray());
      next(ca, dm, T);
      command(ca, "chat.presence.watch", com.pangate.contracts.v1.PresenceWatch.newBuilder()
          .addEntries(com.pangate.contracts.v1.PresenceWatch.Entry.newBuilder().setUserId(b).setConversationId(dm))
          .build().toByteArray());
      var snap = com.pangate.contracts.v1.PresenceUpdate.parseFrom(nextOfType(ca, "chat.presence", T).getPayload());
      if (!snap.getStates(0).getOnline()) {
        throw new IllegalStateException("watched user not online: " + snap);
      }
      t0 = System.nanoTime();
      cb.close();
      var off = com.pangate.contracts.v1.PresenceUpdate.parseFrom(nextOfType(ca, "chat.presence", T.plusSeconds(5))
          .getPayload());
      if (off.getStates(0).getOnline()) {
        throw new IllegalStateException("expected offline: " + off);
      }
      System.out.printf("CHAT presence: online, then offline %.1f s after close (5 s grace)%n",
          (System.nanoTime() - t0) / 1e9);

      if (notify != null) {
        step.accept(STEPS.get(6));
        notifyStep(a, b, dm);
      }
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

  /** Notifications the fake APNs got for {@code token} (CH4 spec §7). */
  int received(String token) throws Exception {
    String body = http.send(HttpRequest.newBuilder(fakeApns.resolve("/received?token=" + token)).timeout(T).build(),
        HttpResponse.BodyHandlers.ofString()).body();
    return body.equals("[]") ? 0 : body.split("\"headers\"").length - 1;
  }

  /**
   * CHAT notify (CH4 spec §0): b, offline since the presence step, registers an APNs token; a's DM reaches the fake
   * APNs once, with b's badge; after b logs that device out, the next DM sends nothing.
   */
  void notifyStep(String a, String b, String dm) throws Exception {
    String token = (UUID.randomUUID().toString() + UUID.randomUUID()).replace("-", "").substring(0, 64);
    String deviceB = byUserId.get(b).deviceId();
    String connectB = token(b);
    int status = NotifyCommand.register(notify.toString(), connectB, "apns", token, "sandbox");
    if (status != 204) {
      throw new IllegalStateException("push token registration answered " + status);
    }
    long t0 = System.nanoTime();
    call("POST", "/v1/conversations/" + dm + "/messages", a, "{\"clientMsgId\":\"" + ulid()
        + "\",\"text\":\"are you there?\"}", ulid());
    long deadline = System.nanoTime() + T.toNanos();
    while (received(token) < 1) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("no notification at the fake APNs");
      }
      Thread.sleep(100);
    }
    String got = http.send(HttpRequest.newBuilder(fakeApns.resolve("/received?token=" + token)).timeout(T).build(),
        HttpResponse.BodyHandlers.ofString()).body();
    // CH4 deferred minor: the success criterion (spec §0) in full: title = a's name, body = the text, badge = b's
    // unread messages across conversations
    @SuppressWarnings("unchecked")
    Map<String, Object> aps = (Map<String, Object>) JSONObjectUtils.parse(String.valueOf(((Map<String, Object>)
        ((List<Object>) JSONObjectUtils.parse("{\"r\":" + got + "}").get("r")).getFirst()).get("body")))
        .get("aps");   // the fake APNs logs {headers, body}; body is the APNs JSON
    @SuppressWarnings("unchecked")
    Map<String, Object> alert = (Map<String, Object>) aps.get("alert");
    long unread = 0;
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> convs = (List<Map<String, Object>>) call("GET", "/v1/conversations", b, null, null)
        .get("conversations");
    for (Map<String, Object> c : convs) {
      unread += ((Number) c.getOrDefault("unread", 0)).longValue();
    }
    String nameA = byUserId.get(a).username();
    if (!"are you there?".equals(alert.get("body")) || !nameA.equalsIgnoreCase(String.valueOf(alert.get("title")))
        || ((Number) aps.get("badge")).longValue() != unread) {
      throw new IllegalStateException("unexpected notification " + got + " (expected title " + nameA + ", badge "
          + unread + ")");
    }
    System.out.printf("CHAT notify: offline DM -> APNs in %.1f ms%n", (System.nanoTime() - t0) / 1e6);
    accounts.logout(connectB, deviceB);
    Thread.sleep(2_000);   // the revocation reaches gw.control, notify-service drops the token
    call("POST", "/v1/conversations/" + dm + "/messages", a, "{\"clientMsgId\":\"" + ulid()
        + "\",\"text\":\"still there?\"}", ulid());
    Thread.sleep(5_000);
    if (received(token) != 1) {
      throw new IllegalStateException("a logged-out device was notified");
    }
  }
}
