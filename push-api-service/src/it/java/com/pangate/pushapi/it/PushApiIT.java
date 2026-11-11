package com.pangate.pushapi.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.contracts.testkit.PushPathScenario;
import com.pangate.contracts.testkit.PushTopics;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.CloseInfo;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.gateway.GatewayApplication;
import com.pangate.pushapi.support.PushApiHarness;
import com.pangate.router.RouterApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The whole downstream path (push-api spec §8): tenant backend → Push API (HTTP) → Kafka → router → gateways → client,
 * outcomes back into the message status; and a gateway KICK through Push API (gRPC) to another gateway. All in this
 * JVM against Testcontainers Kafka and Redis.
 */
class PushApiIT {

  @RegisterExtension static PushPathScenario scenario = new PushPathScenario("c1");
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static final Duration T = Duration.ofSeconds(15);
  static final ObjectMapper JSON = new ObjectMapper();

  @TempDir static Path devDir;
  static PushApiHarness api;
  static ConfigurableApplicationContext router;
  static final List<ConfigurableApplicationContext> gateways = new ArrayList<>();
  static final List<Integer> wsPorts = new ArrayList<>();
  final List<GatewayClient> clients = new ArrayList<>();

  @BeforeAll
  static void boot() throws Exception {
    redis.start();
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", scenario.bootstrapServers()))) {
      try {
        admin.createTopics(List.of(new NewTopic("gw.control", 8, (short) 1))).all().get();
      } catch (Exception e) {
        // exists
      }
    }
    // single session for tenant t1, read by the gateways from gw.control
    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers", scenario.bootstrapServers()),
        new StringSerializer(), new ByteArraySerializer())) {
      p.send(new ProducerRecord<>("gw.control", "tenantcfg:tenant:t1", ControlEvent.newBuilder().setTenantCfg(
          TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
              .setLimits(TenantLimits.newBuilder().setSingleSession(true))).build().toByteArray())).get();
    }
    api = new PushApiHarness(scenario.bootstrapServers(), redisUri());
    gateways.add(startGateway("gw-a"));
    gateways.add(startGateway("gw-b"));
    int mgmt = PushApiHarness.freePort();
    router = new SpringApplicationBuilder(RouterApplication.class).profiles("local")
        .run("--spring.config.name=router", "--server.port=" + mgmt,
            "--router.kafka.bootstrap=" + scenario.bootstrapServers(), "--router.kafka.group=e2e-" + UUID.randomUUID(),
            "--router.registry.uri=" + redisUri(), "--router.ack-timeout=3s");
    awaitReady(mgmt);
  }

  @AfterAll
  static void shutdown() {
    if (router != null) {
      router.close();
    }
    gateways.forEach(ConfigurableApplicationContext::close);
    if (api != null) {
      api.close();
    }
    redis.stop();
  }

  @AfterEach
  void closeClients() {
    for (GatewayClient c : clients) {
      try {
        c.abort();
      } catch (RuntimeException ignored) {
        // closed by the gateway
      }
    }
  }

  static String redisUri() {
    return "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
  }

  static ConfigurableApplicationContext startGateway(String nodeId) throws Exception {
    int mgmt = PushApiHarness.freePort();
    int ws = PushApiHarness.freePort();
    ConfigurableApplicationContext ctx = new SpringApplicationBuilder(GatewayApplication.class).profiles("local")
        .run("--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + ws,
            "--gateway.listeners.tcp.port=" + PushApiHarness.freePort(), "--server.port=" + mgmt,
            "--gateway.io.threads=2", "--gateway.grpc.port=0", "--gateway.warmup.iterations=0",
            "--gateway.node-id=" + nodeId, "--gateway.redis.registry-uri=" + redisUri(),
            "--gateway.redis.state-uri=" + redisUri(), "--gateway.kafka.bootstrap=" + scenario.bootstrapServers(),
            "--gateway.push-api.enabled=true", "--gateway.push-api.target=localhost:" + api.grpc,
            "--gateway.push-api.plaintext=true");
    awaitReady(mgmt);
    await().atMost(T).until(() -> ctx.getBean(com.pangate.gateway.control.TenantLimitsStore.class).policy("t1")
        .singleSession());
    wsPorts.add(ws);
    return ctx;
  }

  static void awaitReady(int mgmt) {
    HttpClient http = HttpClient.newHttpClient();
    await().atMost(Duration.ofSeconds(60)).ignoreExceptions().until(() -> http.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + mgmt + "/ready")).build(),
        HttpResponse.BodyHandlers.discarding()).statusCode() == 200);
  }

  GatewayClient client(int gateway, String principal, String device) {
    GatewayClient c = GatewayClient.connect(URI.create("wss://localhost:" + wsPorts.get(gateway) + "/v1/ws"),
        ClientTls.trust(devDir.resolve("ca.crt")), T);
    clients.add(c);
    String jwt = new JwtMinter(DevKeys.load(devDir.resolve("es256.pem")), "it")
        .mint(new TokenRequest("t1", principal, device, List.of(), Duration.ofMinutes(10)), Instant.now());
    c.hello(Hellos.jwt(jwt, device), T);
    return c;
  }

  static String principal() {
    return "t1:e2e-" + UUID.randomUUID().toString().substring(0, 8);
  }

  static JsonNode status(String token, String id) throws Exception {
    HttpResponse<String> r = api.get(api.http, "/v1/messages/" + id, token);
    return r.statusCode() == 200 ? JSON.readTree(r.body()) : JSON.createObjectNode();
  }

  @Test
  void apiPushReachesTheClientAndStatusIsDelivered() throws Exception {
    String p = principal();
    GatewayClient c = client(0, p, "d1");
    String token = api.token("t1", "t1-backend", "push:send");
    HttpResponse<String> r = api.post(api.http, "/v1/push", token, "{\"target\":{\"type\":\"principal\",\"id\":\"" + p
        + "\"},\"receipt\":true,\"senderPrincipalId\":\"t1:svc\",\"payload\":{\"type\":\"hello\",\"n\":1}}");
    assertThat(r.statusCode()).as(r.body()).isEqualTo(202);
    String id = JSON.readTree(r.body()).get("msgId").asText();
    Frame f = c.awaitPush(T);
    assertThat(f.getMsgId()).isEqualTo(id);
    assertThat(f.getSeq()).isEqualTo(1);
    assertThat(f.getHeadersMap()).containsEntry("type", "hello");
    c.ack(id);
    await().atMost(T).until(() -> status(token, id).path("state").asText().equals("delivered"));
    JsonNode s = status(token, id);
    assertThat(s.get("lastStatus").asText()).isEqualTo("acked");
    assertThat(s.get("deliveredConnections").asLong()).isEqualTo(1);
  }

  @Test
  void offlinePrincipalShowsOfflineNoSession() throws Exception {
    String token = api.token("t1", "t1-backend", "push:send");
    HttpResponse<String> r = api.post(api.http, "/v1/push", token, "{\"target\":{\"type\":\"principal\",\"id\":\""
        + principal() + "\"},\"payload\":{\"n\":2}}");
    String id = JSON.readTree(r.body()).get("msgId").asText();
    await().atMost(T).until(() -> status(token, id).path("state").asText().equals("offline"));
    assertThat(status(token, id).get("offlineReason").asText()).isEqualTo("no_session");
  }

  @Test
  void singleSessionKickGoesThroughPushApi() throws Exception {
    String p = principal();
    GatewayClient older = client(0, p, "d1");
    Thread.sleep(500);   // the older session is registered first
    client(1, p, "d2");  // on the other gateway: its KICK goes Push API → Kafka → router → gw-a
    CloseInfo closed = older.awaitClose(T);
    assertThat(closed.code()).isEqualTo(4012);
    boolean kickProduced = scenario.drain(PushTopics.requests("c1").name()).stream().anyMatch(rec -> {
      try {
        PushRecord pr = PushRecord.parseFrom(rec.value());
        return pr.getKind() == PushRecord.Kind.KIND_CONTROL && pr.getPrincipalId().equals(p)
            && pr.getControl().hasKick();
      } catch (Exception e) {
        return false;
      }
    });
    assertThat(kickProduced).isTrue();
  }
}
