package com.pangate.chat.it;

import static com.pangate.chat.support.ChatHarness.T;
import static com.pangate.chat.support.ChatHarness.command;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.Ids;
import com.pangate.chat.support.ChatHarness;
import com.pangate.contracts.v1.ChannelOpen;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.devtools.client.GatewayClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Measurement for the chat spec §9 success criteria (not part of the normal build): a group of 500 and a channel of
 * 10 000 sessions, send → receive latency per delivery. Run with {@code -Dit.test=ChatLoadIT -Dchat.load=true}.
 */
class ChatLoadIT {

  @TempDir static Path devDir;
  static ChatHarness h;

  @BeforeAll
  static void boot() throws Exception {
    Assumptions.assumeTrue(Boolean.getBoolean("chat.load"), "set -Dchat.load=true to measure");
    h = new ChatHarness(devDir).start();
  }

  @AfterAll
  static void shutdown() {
    if (h != null) {
      h.close();
    }
  }

  /** Every chat.message pushed to these clients: latency from the "t=<nanos>" in its text. */
  static final ConcurrentLinkedQueue<Long> latenciesNs = new ConcurrentLinkedQueue<>();

  static void listen(GatewayClient c) {
    c.onPush(f -> {
      if (f.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
        c.ack(f.getMsgId());
      }
      if ("chat.message".equals(f.getHeadersMap().get("type"))) {
        try {
          String text = ChatMessage.parseFrom(f.getPayload()).getText();
          if (text.startsWith("t=")) {
            latenciesNs.add(System.nanoTime() - Long.parseLong(text.substring(2)));
          }
        } catch (Exception ignored) {
          // not ours
        }
      }
    });
  }

  static List<GatewayClient> connectAll(List<String> users) throws Exception {
    List<GatewayClient> out = Collections.synchronizedList(new ArrayList<>());
    try (ExecutorService pool = Executors.newFixedThreadPool(32)) {
      List<Future<?>> fs = new ArrayList<>();
      for (int i = 0; i < users.size(); i++) {
        int n = i;
        fs.add(pool.submit(() -> {
          GatewayClient c = h.connect(n % h.gateways(), users.get(n), "d1");
          out.add(c);
        }));
      }
      for (Future<?> f : fs) {
        f.get();
      }
    }
    return out;
  }

  static String report(String what, int expected) {
    List<Long> l = new ArrayList<>(latenciesNs);
    Collections.sort(l);
    if (l.isEmpty()) {
      return what + ": nothing received";
    }
    String s = String.format("%s: %d/%d deliveries, send→receive p50 %.1f ms, p99 %.1f ms, max %.1f ms", what,
        l.size(), expected, l.get(l.size() / 2) / 1e6, l.get((int) Math.min(l.size() - 1, l.size() * 0.99)) / 1e6,
        l.getLast() / 1e6);
    System.out.println("CHAT-LOAD " + s);
    io.micrometer.core.instrument.MeterRegistry m = h.chat.getBean(io.micrometer.core.instrument.MeterRegistry.class);
    for (String name : List.of("chat.send.latency", "chat.sequencer.lag", "chat.fanout.lag")) {
      io.micrometer.core.instrument.Timer t = m.find(name).timer();
      if (t != null) {
        System.out.printf("CHAT-LOAD   %s: n=%d mean %.1f ms max %.1f ms%n", name, t.count(),
            t.mean(java.util.concurrent.TimeUnit.MILLISECONDS), t.max(java.util.concurrent.TimeUnit.MILLISECONDS));
      }
    }
    return s;
  }

  static String sendTimed(String user, String conv) {
    HttpResponse<String> r = h.call("POST", "/v1/conversations/" + conv + "/messages", user,
        Map.of("text", "t=" + System.nanoTime()), Map.of("Idempotency-Key", Ids.next()));
    assertThat(r.statusCode()).as(r.body()).isIn(201, 202);
    return r.body();
  }

  @Test
  void groupOf500() throws Exception {
    String run = UUID.randomUUID().toString().substring(0, 6);
    List<String> users = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      users.add("g" + run + "-" + i);
    }
    connectAll(users).forEach(ChatLoadIT::listen);
    String g = h.json(h.call("POST", "/v1/conversations", users.getFirst(), Map.of("kind", "group", "title", "load",
        "memberIds", users.subList(1, users.size())), Map.of())).get("id").asText();
    latenciesNs.clear();
    int messages = 20;
    for (int i = 0; i < messages; i++) {
      sendTimed(users.get(i), g);
      Thread.sleep(200);
    }
    int expected = messages * 500;
    await().atMost(Duration.ofSeconds(120)).until(() -> latenciesNs.size() >= expected);
    report("group 500 (20 messages, 5/s)", expected);
    h.closeClients();
  }

  /**
   * Normal load: {@code chat.load.chats} (default 50) one-to-one chats over WebSocket, each sender sending one message
   * every 500 ms for 10 s.
   */
  @Test
  void directMessagesOverWebSocket() throws Exception {
    String run = UUID.randomUUID().toString().substring(0, 6);
    List<GatewayClient> senders = new ArrayList<>();
    List<String> dms = new ArrayList<>();
    int chats = Integer.getInteger("chat.load.chats", 50);
    for (int i = 0; i < chats; i++) {
      String a = "da" + run + "-" + i;
      String b = "db" + run + "-" + i;
      GatewayClient ca = h.connect(i % h.gateways(), a, "d1");
      GatewayClient cb = h.connect((i + 1) % h.gateways(), b, "d1");
      listen(cb);
      senders.add(ca);
      dms.add(h.json(h.call("POST", "/v1/dm/" + b, a, null, Map.of())).get("id").asText());
    }
    // one message per chat first: the first send of a new conversation reads its last_seq, kind and members
    for (int i = 0; i < senders.size(); i++) {
      command(senders.get(i), "chat.send", com.pangate.contracts.v1.ChatSend.newBuilder()
          .setConversationId(dms.get(i)).setClientMsgId(Ids.next()).setText("warm").build().toByteArray());
    }
    Thread.sleep(3_000);
    latenciesNs.clear();
    int rounds = 20;
    for (int r = 0; r < rounds; r++) {
      for (int i = 0; i < senders.size(); i++) {
        command(senders.get(i), "chat.send", com.pangate.contracts.v1.ChatSend.newBuilder()
            .setConversationId(dms.get(i)).setClientMsgId(Ids.next()).setText("t=" + System.nanoTime()).build()
            .toByteArray());
      }
      Thread.sleep(500);
    }
    int expected = rounds * senders.size();
    await().atMost(Duration.ofSeconds(60)).until(() -> latenciesNs.size() >= expected);
    report("1-1 over WebSocket (" + chats + " chats, " + chats * 2 + " messages/s)", expected);
    h.closeClients();
  }

  static String nowMicrosText() {
    java.time.Instant now = java.time.Instant.now();
    return "w=" + (now.getEpochSecond() * 1_000_000 + now.getNano() / 1_000);
  }

  @Test
  void channelOf10000Sessions() throws Exception {
    String owner = "own-" + UUID.randomUUID().toString().substring(0, 6);
    String ch = h.json(h.call("POST", "/v1/conversations", owner, Map.of("kind", "channel", "title", "load",
        "public", true), Map.of())).get("id").asText();
    List<String> cmd = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xmx2g", "-cp", System.getProperty("java.class.path"), "com.pangate.chat.support.LoadClients",
        h.devDir().toString(), ChatHarness.ISS, "10000", ch));
    h.wsPorts().forEach(p -> cmd.add(String.valueOf(p)));
    Process child = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    java.io.BufferedReader out = new java.io.BufferedReader(new java.io.InputStreamReader(child.getInputStream()));
    java.io.PrintWriter in = new java.io.PrintWriter(child.getOutputStream(), true);
    try {
      long t0 = System.nanoTime();
      String line;
      while ((line = out.readLine()) != null && !line.startsWith("READY")) {
        // child logs
      }
      assertThat(line).as("load clients started").isNotNull();
      System.out.printf("CHAT-LOAD %s sessions connected and opened in %.1f s%n", line.substring(6),
          (System.nanoTime() - t0) / 1e9);
      // the subscriptions travel Push API → Kafka → router → gateways: probe until everyone gets a message
      int reached = 0;
      long deadline = System.nanoTime() + Duration.ofSeconds(180).toNanos();
      while (reached < 10_000 && System.nanoTime() < deadline) {
        in.println("RESET");
        out.readLine();
        post(owner, ch, nowMicrosText());
        Thread.sleep(3_000);
        in.println("COUNT");
        reached = Integer.parseInt(out.readLine().split(" ")[1]);
        System.out.println("CHAT-LOAD probe reached " + reached);
      }
      in.println("RESET");
      out.readLine();
      int messages = 10;
      for (int i = 0; i < messages; i++) {
        post(owner, ch, nowMicrosText());
        Thread.sleep(500);
      }
      Thread.sleep(10_000);
      in.println("REPORT");
      String[] r = out.readLine().split(" ");
      String s = String.format("channel 10 000 sessions (10 messages, 2/s): %s/%d deliveries, send→receive p50 %.1f "
          + "ms, p99 %.1f ms, max %.1f ms", r[1], messages * 10_000, Long.parseLong(r[2]) / 1e3,
          Long.parseLong(r[3]) / 1e3, Long.parseLong(r[4]) / 1e3);
      System.out.println("CHAT-LOAD " + s);
      in.println("EXIT");
    } finally {
      child.destroyForcibly();
    }
  }

  static void post(String user, String conv, String text) {
    HttpResponse<String> r = h.call("POST", "/v1/conversations/" + conv + "/messages", user, Map.of("text", text),
        Map.of("Idempotency-Key", Ids.next()));
    assertThat(r.statusCode()).as(r.body()).isIn(201, 202);
  }
}
