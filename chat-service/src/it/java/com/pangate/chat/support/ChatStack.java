package com.pangate.chat.support;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.pangate.chat.core.Ids;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.pushapi.PushApiApplication;
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
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Kafka, ScyllaDB, Redis, a JWKS endpoint, the Push API and chat-service, all in this JVM or in containers. One per
 * test class: {@link #start} in {@code @BeforeAll}, {@link #close} in {@code @AfterAll} (ScyllaDB holds kernel AIO
 * slots; a container left running starves the next one).
 */
public final class ChatStack implements AutoCloseable {

  public static final String ISS = "https://auth.pangate";
  public static final ObjectMapper JSON = new ObjectMapper();

  public final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  public final ScyllaDBContainer scylla = new ScyllaDBContainer("scylladb/scylla:6.2.3");
  public final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  public final ECKey key;
  public final HttpClient http = HttpClient.newHttpClient();
  private HttpServer jwks;
  public ConfigurableApplicationContext pushApi;
  public ConfigurableApplicationContext chat;
  public int pushGrpc;
  public int chatHttp;
  public int chatMgmt;

  public ChatStack() {
    try {
      key = new ECKeyGenerator(Curve.P_256).keyID("k1").generate();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static final int PORTS_FROM = 20_000;
  /** Linux hands out outbound (ephemeral) ports from 32768: none of ours may be taken by a client in between. */
  private static final int PORTS_TO = 32_768;
  private static final java.util.concurrent.atomic.AtomicInteger NEXT_PORT = new java.util.concurrent.atomic
      .AtomicInteger(PORTS_FROM + java.util.concurrent.ThreadLocalRandom.current().nextInt(8_000));

  /**
   * A port that is free now, below the ephemeral range and never handed out twice in this JVM. A port from
   * {@code new ServerSocket(0)} is an ephemeral one: between this call and the server's bind, a Kafka, ScyllaDB or Redis
   * client connection could take it as its source port ("Address already in use").
   */
  public static int freePort() {
    for (int i = 0; i < PORTS_TO - PORTS_FROM; i++) {
      int p = NEXT_PORT.getAndUpdate(n -> n + 1 >= PORTS_TO ? PORTS_FROM : n + 1);
      try (ServerSocket s = new ServerSocket()) {
        s.bind(new java.net.InetSocketAddress(p));
        return p;
      } catch (java.io.IOException taken) {
        // next one
      }
    }
    throw new IllegalStateException("no free port in " + PORTS_FROM + ".." + PORTS_TO);
  }

  public String redisUri() {
    return "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
  }

  public ChatStack start(String... chatArgs) throws Exception {
    kafka.start();
    scylla.start();
    redis.start();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic("gw.control", 2, (short) 1), new NewTopic(ChatTopics.COMMANDS, 4, (short) 1),
          new NewTopic(ChatTopics.EVENTS, 4, (short) 1), new NewTopic(ChatTopics.DLQ, 1, (short) 1),
          new NewTopic("c1.gw.upstream.cmd", 2, (short) 1), new NewTopic("c1.push.requests", 4, (short) 1)))
          .all().get();
    }
    jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    byte[] body = new JWKSet(List.of(key.toPublicJWK())).toString().getBytes(StandardCharsets.UTF_8);
    jwks.createContext("/jwks", ex -> {
      ex.sendResponseHeaders(200, body.length);
      ex.getResponseBody().write(body);
      ex.close();
    });
    jwks.start();
    pushGrpc = freePort();
    pushApi = new SpringApplicationBuilder(PushApiApplication.class).profiles("local").run(
        "--spring.config.name=push-api", "--server.port=" + freePort(), "--push-api.management-port=" + freePort(),
        "--push-api.grpc.port=" + pushGrpc, "--push-api.kafka.bootstrap=" + kafka.getBootstrapServers(),
        "--push-api.redis.uri=" + redisUri(), "--push-api.auth.jwks-uri=http://127.0.0.1:1/none",
        "--spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration");
    chatHttp = freePort();
    chatMgmt = freePort();
    String cp = scylla.getContactPoint().getHostString() + ":" + scylla.getContactPoint().getPort();
    List<String> args = new ArrayList<>(List.of("--server.port=" + chatHttp, "--chat.management-port=" + chatMgmt,
        "--chat.kafka.bootstrap=" + kafka.getBootstrapServers(), "--chat.scylla.contact-points=" + cp,
        "--chat.redis.uri=" + redisUri(), "--chat.auth.issuer=" + ISS,
        "--chat.auth.jwks-uri=http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks",
        "--chat.push-api.target=127.0.0.1:" + pushGrpc, "--chat.push-api.plaintext=true"));
    args.addAll(List.of(chatArgs));
    chat = new SpringApplicationBuilder(ChatApplication.class).properties(ChatApplication.CONFIG_NAME)
        .run(args.toArray(String[]::new));
    await().atMost(Duration.ofSeconds(60)).until(() -> status("GET", chatMgmt, "/ready", null, null) == 200);
    return this;
  }

  public String token(String userId, String device) {
    long now = System.currentTimeMillis() - 1_000;
    try {
      JWTClaimsSet c = new JWTClaimsSet.Builder().issuer(ISS).audience("pangate-gw").subject("chat:" + userId)
          .claim("tenant", "chat").claim("device", device).jwtID(Ids.next()).issueTime(new Date(now))
          .notBeforeTime(new Date(now)).expirationTime(new Date(now + 900_000)).build();
      SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("k1").type(JOSEObjectType.JWT)
          .build(), c);
      jwt.sign(new ECDSASigner(key));
      return jwt.serialize();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public HttpResponse<String> call(String method, String path, String userId, Object body) {
    return call(method, path, userId == null ? null : token(userId, "d1"), body, Map.of());
  }

  public HttpResponse<String> call(String method, String path, String token, Object body, Map<String, String> h) {
    try {
      HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + chatHttp + path));
      if (token != null) {
        b.header("Authorization", "Bearer " + token);
      }
      h.forEach(b::header);
      if (body != null) {
        b.header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
      } else {
        b.method(method, HttpRequest.BodyPublishers.noBody());
      }
      return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  public JsonNode json(HttpResponse<String> r) {
    try {
      return JSON.readTree(r.body());
    } catch (java.io.IOException e) {
      throw new IllegalStateException(r.body(), e);
    }
  }

  int status(String method, int port, String path, String token, Object body) throws Exception {
    return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
        HttpResponse.BodyHandlers.discarding()).statusCode();
  }

  /** Sends {@code text} over HTTP and returns the 201 body (waits for the sequencer). */
  public JsonNode send(String userId, String convId, String text) {
    HttpResponse<String> r = call("POST", "/v1/conversations/" + convId + "/messages", token(userId, "d1"),
        Map.of("text", text), Map.of("Idempotency-Key", Ids.next()));
    if (r.statusCode() != 201) {
      throw new AssertionError("send answered " + r.statusCode() + " " + r.body());
    }
    return json(r);
  }

  @Override
  public void close() {
    if (chat != null) {
      chat.close();
    }
    if (pushApi != null) {
      pushApi.close();
    }
    if (jwks != null) {
      jwks.stop(0);
    }
    redis.stop();
    scylla.stop();
    kafka.stop();
  }
}
