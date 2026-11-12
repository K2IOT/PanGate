package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.jwk.JWKSet;
import com.pangate.chat.ChatApplication;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.Ids;
import com.pangate.chat.store.ScyllaChatStore;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** HTTP send waits for the sequencer (chat spec §4.2): 201 with the result, 202 on timeout or without Redis. */
class SendHttpIT {

  static final ObjectMapper JSON = new ObjectMapper();
  static HttpServer jwks;

  @BeforeAll
  static void boot() throws Exception {
    SendAcceptIT.kafka.start();
    SendAcceptIT.scylla.start();
    SendAcceptIT.redis.start();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", SendAcceptIT.kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic("gw.control", 2, (short) 1), new NewTopic(ChatTopics.COMMANDS, 4, (short) 1),
          new NewTopic(ChatTopics.EVENTS, 4, (short) 1), new NewTopic(ChatTopics.DLQ, 1, (short) 1),
          new NewTopic("c1.gw.upstream.cmd", 2, (short) 1))).all().get();
    }
    jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    byte[] body = new JWKSet(List.of(SendAcceptIT.key.toPublicJWK())).toString().getBytes(StandardCharsets.UTF_8);
    jwks.createContext("/jwks", ex -> {
      ex.sendResponseHeaders(200, body.length);
      ex.getResponseBody().write(body);
      ex.close();
    });
    jwks.start();
  }

  @AfterAll
  static void shutdown() {
    jwks.stop(0);
    // ScyllaDB holds kernel AIO slots: a container left running makes the next IT's ScyllaDB fail to start
    SendAcceptIT.redis.stop();
    SendAcceptIT.scylla.stop();
    SendAcceptIT.kafka.stop();
  }

  /** A context; {@code http} port is its first int. */
  record App(ConfigurableApplicationContext ctx, int http, int mgmt) implements AutoCloseable {
    @Override
    public void close() {
      ctx.close();
    }
  }

  static App app(String redisUri, String... extra) throws Exception {
    int http = SendAcceptIT.freePort();
    int mgmt = SendAcceptIT.freePort();
    String cp = SendAcceptIT.scylla.getContactPoint().getHostString() + ":"
        + SendAcceptIT.scylla.getContactPoint().getPort();
    List<String> args = new ArrayList<>(List.of("--server.port=" + http, "--chat.management-port=" + mgmt,
        "--chat.kafka.bootstrap=" + SendAcceptIT.kafka.getBootstrapServers(), "--chat.scylla.contact-points=" + cp,
        "--chat.redis.uri=" + redisUri, "--chat.auth.issuer=" + SendAcceptIT.ISS,
        "--chat.auth.jwks-uri=http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks"));
    args.addAll(List.of(extra));
    ConfigurableApplicationContext ctx = new SpringApplicationBuilder(ChatApplication.class)
        .properties(ChatApplication.CONFIG_NAME).run(args.toArray(String[]::new));
    App a = new App(ctx, http, mgmt);
    await().atMost(Duration.ofSeconds(60)).until(() -> SendAcceptIT.get(mgmt, "/ready") == 200);
    return a;
  }

  static String goodRedis() {
    return "redis://" + SendAcceptIT.redis.getHost() + ":" + SendAcceptIT.redis.getMappedPort(6379);
  }

  static String conv(App a) {
    String id = Ids.next();
    a.ctx().getBean(ScyllaChatStore.class).create(new Conversation(id, ConversationKind.CONVERSATION_KIND_GROUP,
        "g", "u1", 0, false, 1), List.of(new Member("u1", Role.MEMBER)));
    return id;
  }

  static String token() throws Exception {
    return SendAcceptIT.token("chat:u1", "d1", System.currentTimeMillis() - 1_000);
  }

  @Test
  void sendWaitsForResult201() throws Exception {
    try (App a = app(goodRedis())) {
      String c = conv(a);
      String k = Ids.next();
      HttpResponse<String> r = SendAcceptIT.send(a.http(), c, token(), k);
      assertThat(r.statusCode()).as(r.body()).isEqualTo(201);
      JsonNode b = JSON.readTree(r.body());
      assertThat(Ids.isUlid(b.get("msgId").asText())).isTrue();
      assertThat(b.get("convSeq").asLong()).isEqualTo(1);
      assertThat(b.get("sentAt").asLong()).isPositive();
      // the same key again: the same message, no new seq
      HttpResponse<String> again = SendAcceptIT.send(a.http(), c, token(), k);
      assertThat(again.statusCode()).isEqualTo(201);
      assertThat(JSON.readTree(again.body()).get("msgId").asText()).isEqualTo(b.get("msgId").asText());
      assertThat(JSON.readTree(SendAcceptIT.send(a.http(), c, token(), Ids.next()).body()).get("convSeq").asLong())
          .isEqualTo(2);
    }
  }

  @Test
  void sendTimesOut202ThenSameKeyGets201() throws Exception {
    try (App a = app(goodRedis(), "--chat.http.send-wait=1ms")) {
      String c = conv(a);
      String k = Ids.next();
      HttpResponse<String> r = SendAcceptIT.send(a.http(), c, token(), k);
      assertThat(r.statusCode()).isEqualTo(202);
      assertThat(JSON.readTree(r.body()).get("clientMsgId").asText()).isEqualTo(k);
      await().atMost(Duration.ofSeconds(20)).until(() -> SendAcceptIT.send(a.http(), c, token(), k).statusCode() == 201);
      assertThat(a.ctx().getBean(ScyllaChatStore.class).lastSeq(c)).isEqualTo(1);
    }
  }

  @Test
  void redisDownGives202() throws Exception {
    try (App a = app("redis://127.0.0.1:1")) {
      String c = conv(a);
      String k = Ids.next();
      long t0 = System.nanoTime();
      HttpResponse<String> r = SendAcceptIT.send(a.http(), c, token(), k);
      assertThat(r.statusCode()).isEqualTo(202);
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(2));
      // ScyllaDB dedup still answers the repeat once the message is in
      await().atMost(Duration.ofSeconds(20)).until(() -> SendAcceptIT.send(a.http(), c, token(), k).statusCode() == 201);
      assertThat(a.ctx().getBean(ScyllaChatStore.class).lastSeq(c)).isEqualTo(1);
    }
  }
}
