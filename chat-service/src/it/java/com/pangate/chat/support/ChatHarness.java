package com.pangate.chat.support;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.chat.ChatApplication;
import com.pangate.chat.core.Ids;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.gateway.GatewayApplication;
import com.pangate.pushapi.PushApiApplication;
import com.pangate.router.RouterApplication;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The whole chat path in one JVM (chat spec §9): client → gateway (WebSocket) → Kafka → chat-service → Push API →
 * router → gateways → clients, against Testcontainers Kafka, Redis and ScyllaDB. Connect tokens are minted with the
 * gateways' dev key; chat-service reads the same key from a JWKS endpoint, as it would from auth-service.
 */
public final class ChatHarness implements AutoCloseable {

  public static final String ISS = "https://auth.pangate";
  public static final Duration T = Duration.ofSeconds(15);
  public static final ObjectMapper JSON = new ObjectMapper();

  final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  final ScyllaDBContainer scylla = new ScyllaDBContainer("scylladb/scylla:6.2.3");
  final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  final HttpClient http = HttpClient.newHttpClient();
  final List<ConfigurableApplicationContext> apps = new ArrayList<>();
  final List<Integer> wsPorts = new ArrayList<>();
  final List<GatewayClient> clients = new ArrayList<>();
  final Path devDir;
  HttpServer jwks;
  JwtMinter minter;
  public ConfigurableApplicationContext chat;
  int chatHttp;

  public ChatHarness(Path devDir) {
    this.devDir = devDir;
  }

  String redisUri() {
    return "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
  }

  void awaitReady(int mgmt) {
    await().atMost(Duration.ofSeconds(90)).ignoreExceptions().until(() -> http.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + mgmt + "/ready")).build(),
        HttpResponse.BodyHandlers.discarding()).statusCode() == 200);
  }

  public ChatHarness start() throws Exception {
    kafka.start();
    redis.start();
    scylla.start();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic("gw.control", 4, (short) 1), new NewTopic(ChatTopics.COMMANDS, 8, (short) 1),
          new NewTopic(ChatTopics.EVENTS, 8, (short) 1), new NewTopic(ChatTopics.DLQ, 1, (short) 1),
          new NewTopic("c1.gw.upstream.cmd", 4, (short) 1), new NewTopic("c1.push.requests", 8, (short) 1)))
          .all().get();
    }
    String kafkaBs = kafka.getBootstrapServers();
    // tenant chat's push limits (chat spec §8): a group message is up to 500 pushes
    try (org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> p =
        new org.apache.kafka.clients.producer.KafkaProducer<>(Map.of("bootstrap.servers", kafkaBs),
            new org.apache.kafka.common.serialization.StringSerializer(),
            new org.apache.kafka.common.serialization.ByteArraySerializer())) {
      p.send(new org.apache.kafka.clients.producer.ProducerRecord<>("gw.control", "tenantcfg:tenant:chat",
          com.pangate.contracts.v1.ControlEvent.newBuilder().setTenantCfg(com.pangate.contracts.v1.TenantCfg
              .newBuilder().setScope(com.pangate.contracts.v1.Scope.SCOPE_TENANT).setScopeId("chat")
              .setLimits(com.pangate.contracts.v1.TenantLimits.newBuilder().setPushRate(20_000)
                  .setPushBurst(40_000))).build().toByteArray())).get();
    }
    int pushGrpc = ChatStack.freePort();
    int pushMgmt = ChatStack.freePort();
    apps.add(new SpringApplicationBuilder(PushApiApplication.class).profiles("local").run(
        "--spring.config.name=push-api", "--server.port=" + ChatStack.freePort(),
        "--push-api.management-port=" + pushMgmt, "--push-api.grpc.port=" + pushGrpc,
        "--push-api.kafka.bootstrap=" + kafkaBs, "--push-api.redis.uri=" + redisUri(),
        "--push-api.auth.jwks-uri=http://127.0.0.1:1/none", "--push-api.kafka.status-group=e2e-" + UUID.randomUUID(),
        "--spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration"));
    for (String node : List.of("gw-a", "gw-b")) {
      int mgmt = ChatStack.freePort();
      int ws = ChatStack.freePort();
      apps.add(new SpringApplicationBuilder(GatewayApplication.class).profiles("local").run(
          "--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + ws,
          "--gateway.listeners.tcp.port=" + ChatStack.freePort(), "--server.port=" + mgmt, "--gateway.io.threads=2",
          "--gateway.grpc.port=0", "--gateway.warmup.iterations=0", "--gateway.node-id=" + node,
          "--gateway.redis.registry-uri=" + redisUri(), "--gateway.redis.state-uri=" + redisUri(),
          "--gateway.kafka.bootstrap=" + kafkaBs, "--gateway.push-api.enabled=true",
          "--gateway.push-api.target=localhost:" + pushGrpc, "--gateway.push-api.plaintext=true",
          "--spring.autoconfigure.exclude="
              + "org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration"));
      awaitReady(mgmt);
      wsPorts.add(ws);
    }
    int routerMgmt = ChatStack.freePort();
    apps.add(new SpringApplicationBuilder(RouterApplication.class).profiles("local").run(
        "--spring.config.name=router", "--server.port=" + routerMgmt, "--router.kafka.bootstrap=" + kafkaBs,
        "--router.kafka.group=e2e-" + UUID.randomUUID(), "--router.registry.uri=" + redisUri(),
        "--router.ack-timeout=3s",
        "--spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration"));
    awaitReady(routerMgmt);
    // Push API's /ready also wants its own JWKS (API tokens), which this harness does not serve; its gRPC
    // SendStream, all chat-service and the gateways use, is up once the context is.
    // the gateways bootstrapped the dev key: chat-service gets it as auth-service's JWKS
    minter = new JwtMinter(DevKeys.load(devDir.resolve("es256.pem")), ISS);
    byte[] body = Files.readAllBytes(devDir.resolve("jwks.json"));
    jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    jwks.createContext("/jwks", ex -> {
      ex.sendResponseHeaders(200, body.length);
      ex.getResponseBody().write(body);
      ex.close();
    });
    jwks.start();
    chatHttp = ChatStack.freePort();
    int chatMgmt = ChatStack.freePort();
    String cp = scylla.getContactPoint().getHostString() + ":" + scylla.getContactPoint().getPort();
    chat = new SpringApplicationBuilder(ChatApplication.class).properties(ChatApplication.CONFIG_NAME).run(
        "--server.port=" + chatHttp, "--chat.management-port=" + chatMgmt, "--chat.kafka.bootstrap=" + kafkaBs,
        "--chat.scylla.contact-points=" + cp, "--chat.redis.uri=" + redisUri(), "--chat.auth.issuer=" + ISS,
        "--chat.auth.jwks-uri=http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks",
        "--chat.push-api.target=127.0.0.1:" + pushGrpc, "--chat.push-api.plaintext=true");
    apps.add(chat);
    awaitReady(chatMgmt);
    return this;
  }

  public String token(String userId, String device) {
    return minter.mint(new TokenRequest("chat", "chat:" + userId, device, List.of(), Duration.ofMinutes(10)),
        Instant.now().minusSeconds(1));
  }

  private ClientTls tls;

  synchronized ClientTls tls() {
    if (tls == null) {
      tls = ClientTls.trust(devDir.resolve("ca.crt"));
    }
    return tls;
  }

  /** A WebSocket session of {@code userId} on gateway {@code gw} (0 or 1). */
  public GatewayClient connect(int gw, String userId, String device) {
    GatewayClient c = GatewayClient.connect(URI.create("wss://localhost:" + wsPorts.get(gw) + "/v1/ws"), tls(), T);
    clients.add(c);
    c.hello(Hellos.jwt(token(userId, device), device), T);
    return c;
  }

  /** {@code op} with {@code payload} as UPSTREAM type {@code cmd}; returns the gateway's UPSTREAM_ACK or ERROR. */
  public static Frame command(GatewayClient c, String op, byte[] payload) {
    String id = UUID.randomUUID().toString();
    c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId(id).putHeaders("type", "cmd")
        .putHeaders("op", op).setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build());
    return c.await(FrameType.FRAME_TYPE_UPSTREAM_ACK, T);
  }

  public static ChatSend chatSend(String convId, String text) {
    return ChatSend.newBuilder().setConversationId(convId).setClientMsgId(Ids.next()).setText(text).build();
  }

  public HttpResponse<String> call(String method, String path, String userId, Object body, Map<String, String> h) {
    try {
      HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + chatHttp + path))
          .header("Authorization", "Bearer " + token(userId, "http"));
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

  public int gateways() {
    return wsPorts.size();
  }

  public List<Integer> wsPorts() {
    return List.copyOf(wsPorts);
  }

  public Path devDir() {
    return devDir;
  }

  public void closeClients() {
    for (GatewayClient c : clients) {
      try {
        c.abort();
      } catch (RuntimeException ignored) {
        // already closed
      }
    }
    clients.clear();
  }

  @Override
  public void close() {
    closeClients();
    for (int i = apps.size() - 1; i >= 0; i--) {
      apps.get(i).close();
    }
    if (jwks != null) {
      jwks.stop(0);
    }
    scylla.stop();
    redis.stop();
    kafka.stop();
  }
}
