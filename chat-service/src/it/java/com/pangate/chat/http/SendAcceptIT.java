package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.chat.ChatApplication;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.Ids;
import com.pangate.chat.store.ScyllaChatStore;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Revoke;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

/** The whole service on real Kafka, ScyllaDB and Redis: auth, ports, readiness and the accepted send path. */
class SendAcceptIT {

  static final String ISS = "https://auth.pangate";
  static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  static final ScyllaDBContainer scylla = new ScyllaDBContainer("scylladb/scylla:6.2.3");
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static final ECKey key = gen();
  static HttpServer jwks;
  static ConfigurableApplicationContext ctx;
  static int http;
  static int mgmt;
  static final HttpClient client = HttpClient.newHttpClient();

  static ECKey gen() {
    try {
      return new ECKeyGenerator(Curve.P_256).keyID("k1").generate();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  @BeforeAll
  static void boot() throws Exception {
    kafka.start();
    scylla.start();
    redis.start();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic("gw.control", 2, (short) 1), new NewTopic(ChatTopics.COMMANDS, 4, (short) 1),
          new NewTopic(ChatTopics.DLQ, 1, (short) 1), new NewTopic("c1.gw.upstream.cmd", 2, (short) 1))).all().get();
    }
    jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    byte[] body = new JWKSet(List.of(key.toPublicJWK())).toString().getBytes(StandardCharsets.UTF_8);
    jwks.createContext("/jwks", ex -> {
      ex.sendResponseHeaders(200, body.length);
      ex.getResponseBody().write(body);
      ex.close();
    });
    jwks.start();
    http = freePort();
    mgmt = freePort();
    String cp = scylla.getContactPoint().getHostString() + ":" + scylla.getContactPoint().getPort();
    ctx = new SpringApplicationBuilder(ChatApplication.class).properties(ChatApplication.CONFIG_NAME).run(
        "--server.port=" + http, "--chat.management-port=" + mgmt,
        "--chat.kafka.bootstrap=" + kafka.getBootstrapServers(), "--chat.scylla.contact-points=" + cp,
        "--chat.redis.uri=redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
        "--chat.auth.jwks-uri=http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks", "--chat.auth.issuer=" + ISS);
  }

  @AfterAll
  static void shutdown() {
    if (ctx != null) {
      ctx.close();
    }
    jwks.stop(0);
    redis.stop();
    scylla.stop();
    kafka.stop();
  }

  static String token(String principal, String device, long iatMs) throws Exception {
    JWTClaimsSet c = new JWTClaimsSet.Builder().issuer(ISS).audience("pangate-gw").subject(principal)
        .claim("tenant", "chat").claim("device", device).jwtID(Ids.next()).issueTime(new Date(iatMs))
        .notBeforeTime(new Date(iatMs)).expirationTime(new Date(iatMs + 900_000)).build();
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("k1").type(JOSEObjectType.JWT)
        .build(), c);
    jwt.sign(new ECDSASigner(key));
    return jwt.serialize();
  }

  static HttpResponse<String> send(int port, String conv, String token, String key) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/conversations/"
        + conv + "/messages")).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{\"text\":\"hello\"}"));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    if (key != null) {
      b.header("Idempotency-Key", key);
    }
    return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  static int get(int port, String path) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
        HttpResponse.BodyHandlers.discarding()).statusCode();
  }

  @Test
  void readySendAcceptedAndRevokedDeviceRefused() throws Exception {
    await().atMost(Duration.ofSeconds(60)).until(() -> get(mgmt, "/ready") == 200);
    assertThat(get(http, "/ready")).as("management paths only on the management port").isEqualTo(404);

    String conv = Ids.next();
    ctx.getBean(ScyllaChatStore.class).create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g",
        "u1", 0, false, 1), List.of(new Member("u1", Role.MEMBER)));
    String t = token("chat:u1", "d1", System.currentTimeMillis() - 1_000);
    String k = Ids.next();

    assertThat(send(http, conv, null, k).statusCode()).isEqualTo(401);
    HttpResponse<String> ok = send(http, conv, t, k);
    assertThat(ok.statusCode()).isIn(201, 202);   // 201 once the sequencer answers within the wait
    assertThat(ok.body()).contains(ok.statusCode() == 202 ? k : "\"convSeq\":1");
    assertThat(send(http, Ids.next(), t, Ids.next()).statusCode()).isEqualTo(404);
    assertThat(send(http, conv, token("chat:u2", "d1", System.currentTimeMillis() - 1_000), Ids.next())
        .statusCode()).isEqualTo(403);

    List<ConsumerRecord<String, byte[]>> cmds = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers(), "group.id", "check", "auto.offset.reset", "earliest"),
        new StringDeserializer(), new ByteArrayDeserializer())) {
      c.subscribe(List.of(ChatTopics.COMMANDS));
      long deadline = System.currentTimeMillis() + 20_000;
      while (cmds.isEmpty() && System.currentTimeMillis() < deadline) {
        c.poll(Duration.ofMillis(200)).forEach(cmds::add);
      }
    }
    assertThat(cmds).singleElement().satisfies(r -> {
      assertThat(r.key()).isEqualTo(conv);
      assertThat(ChatCommand.parseFrom(r.value()).getSend().getClientMsgId()).isEqualTo(k);
    });

    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
      p.send(new ProducerRecord<>("gw.control", "revoke:d:chat:u1/d1", ControlEvent.newBuilder()
          .setIssuedAtMs(System.currentTimeMillis()).setRevoke(Revoke.newBuilder().setDevice(
              DeviceRef.newBuilder().setPrincipalId("chat:u1").setDeviceId("d1"))).build().toByteArray())).get();
    }
    await().atMost(Duration.ofSeconds(20)).until(() -> send(http, conv, t, Ids.next()).statusCode() == 403);
  }
}
