package com.pangate.router.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.PushPathScenario;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.CloseInfo;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.gateway.GatewayApplication;
import com.pangate.gateway.registry.NodeIdentity;
import com.pangate.router.RouterApplication;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * The push path end to end (push-router spec §7): PushRecord on Kafka → router → real gateway (gRPC) → WebSocket client,
 * and the outcomes back on Kafka. Both applications run in this JVM against Testcontainers Kafka and Redis.
 */
class RouterIT {

  @RegisterExtension static PushPathScenario scenario = new PushPathScenario("c1");
  static final GenericContainer<?> redis =
      new GenericContainer<>(DockerImageName.parse("redis:7.4.2")).withExposedPorts(6379);
  static final Duration T = Duration.ofSeconds(15);

  @TempDir static Path devDir;
  static ConfigurableApplicationContext gateway;
  static ConfigurableApplicationContext router;
  static int ws;
  static final String GROUP = "router-it-" + UUID.randomUUID();
  final List<GatewayClient> clients = new ArrayList<>();

  @BeforeAll
  static void boot() throws Exception {
    redis.start();
    gateway = startGateway("gw-r1");
    router = startRouter("2s");
  }

  @AfterAll
  static void shutdown() {
    if (router != null) {
      router.close();
    }
    if (gateway != null) {
      gateway.close();
    }
    redis.stop();
  }

  @AfterEach
  void closeClients() {
    for (GatewayClient c : clients) {
      try {
        c.abort();
      } catch (RuntimeException ignored) {
        // already closed by the gateway
      }
    }
  }

  static String redisUri() {
    return "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
  }

  static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }

  static ConfigurableApplicationContext startGateway(String nodeId) throws Exception {
    int mgmt = freePort();
    int wsPort = freePort();
    ConfigurableApplicationContext ctx = new SpringApplicationBuilder(GatewayApplication.class).profiles("local")
        .run("--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + wsPort,
            "--gateway.listeners.tcp.port=" + freePort(), "--server.port=" + mgmt, "--gateway.io.threads=2",
            "--gateway.grpc.port=0", "--gateway.warmup.iterations=0", "--gateway.node-id=" + nodeId,
            "--gateway.redis.registry-uri=" + redisUri(), "--gateway.redis.state-uri=" + redisUri(),
            "--gateway.kafka.bootstrap=" + scenario.bootstrapServers());
    awaitReady(mgmt);
    if (gateway == null) {
      ws = wsPort;
    }
    return ctx;
  }

  static ConfigurableApplicationContext startRouter(String ackTimeout) throws Exception {
    int mgmt = freePort();
    ConfigurableApplicationContext ctx = new SpringApplicationBuilder(RouterApplication.class).profiles("local")
        .run("--spring.config.name=router", "--server.port=" + mgmt,
            "--router.kafka.bootstrap=" + scenario.bootstrapServers(), "--router.kafka.group=" + GROUP,
            "--router.registry.uri=" + redisUri(), "--router.ack-timeout=" + ackTimeout);
    awaitReady(mgmt);
    return ctx;
  }

  static void awaitReady(int mgmt) {
    HttpClient http = HttpClient.newHttpClient();
    await().atMost(Duration.ofSeconds(30)).ignoreExceptions().until(() -> http.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + mgmt + "/ready")).build(),
        HttpResponse.BodyHandlers.discarding()).statusCode() == 200);
  }

  // --- helpers -------------------------------------------------------------------------------------------------

  GatewayClient client(String principal, String device) {
    return client(principal, device, List.of());
  }

  /** {@code topics}: the token's topic claim, which SUBSCRIBE is checked against. */
  GatewayClient client(String principal, String device, List<String> topics) {
    GatewayClient c = GatewayClient.connect(URI.create("wss://localhost:" + ws + "/v1/ws"),
        ClientTls.trust(devDir.resolve("ca.crt")), T);
    clients.add(c);
    String tenant = principal.substring(0, principal.indexOf(':'));
    String jwt = new JwtMinter(DevKeys.load(devDir.resolve("es256.pem")), "it")
        .mint(new TokenRequest(tenant, principal, device, topics, Duration.ofMinutes(10)), Instant.now());
    connected.put(c, c.hello(Hellos.jwt(jwt, device), T));
    return c;
  }

  final java.util.Map<GatewayClient, Connected> connected = new java.util.HashMap<>();

  /** Fresh times (the fixtures' clock is in the past) and a unique msg_id. */
  static PushRecord fresh(PushRecord r) {
    long now = System.currentTimeMillis();
    return Fixtures.withMsgId(r, UUID.randomUUID().toString()).toBuilder().setAcceptedAtMs(now)
        .setExpiresAtMs(now + 60_000).build();
  }

  static String principal() {
    return "t1:u-" + UUID.randomUUID().toString().substring(0, 8);
  }

  // --- tests ---------------------------------------------------------------------------------------------------

  @Test
  void clientReceivesPushAndAckRecordsDelivered() {
    String p = principal();
    GatewayClient c = client(p, "d1");
    PushRecord r = fresh(Fixtures.message(p)).toBuilder().setReceipt(true).setSenderPrincipalId("t1:svc").build();
    scenario.produce(r);
    Frame f = c.awaitPush(T);
    assertThat(f.getMsgId()).isEqualTo(r.getMsgId());
    c.ack(f.getMsgId());
    DeliveredRecord d = scenario.awaitDelivered(r.getMsgId());
    assertThat(d.getStatus()).isEqualTo(AckItem.Status.STATUS_ACKED);
    assertThat(d.getConnId()).isEqualTo(connected.get(c).getConnId());
  }

  @Test
  void offlinePrincipalProducesOfflineNoSession() {
    PushRecord r = fresh(Fixtures.message(principal()));
    scenario.produce(r);
    OfflineRecord o = scenario.awaitOffline(r.getMsgId(), OfflineRecord.Reason.REASON_NO_SESSION);
    assertThat(o.getAttempts()).isZero();
  }

  @Test
  void silentClientTimesOutToOfflineWithThreeAttempts() {
    String p = principal();
    GatewayClient c = client(p, "d1");
    PushRecord r = fresh(Fixtures.message(p));
    scenario.produce(r);
    assertThat(c.awaitPush(T).getMsgId()).isEqualTo(r.getMsgId());   // received, never acked
    OfflineRecord o = scenario.awaitOffline(r.getMsgId(), OfflineRecord.Reason.REASON_NACK_TIMEOUT);
    assertThat(o.getAttempts()).isEqualTo(3);
  }

  @Test
  void deviceTargetReachesOnlyThatDevice() {
    String p = principal();
    GatewayClient d1 = client(p, "d1");
    GatewayClient d2 = client(p, "d2");
    PushRecord r = fresh(Fixtures.deviceMessage(p, "d2"));
    scenario.produce(r);
    Frame f = d2.awaitPush(T);
    assertThat(f.getMsgId()).isEqualTo(r.getMsgId());
    d2.ack(f.getMsgId());
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> d1.awaitPush(Duration.ofSeconds(2)))
        .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
  }

  @Test
  void broadcastReachesSubscribers() {
    String p = principal();
    GatewayClient c = client(p, "d1", List.of("t1:news"));
    GatewayClient other = client(principal(), "d1", List.of("t1:news"));
    c.subscribe("t1:news");
    c.ping(T);   // SUBSCRIBE handled before PONG
    PushRecord r = fresh(Fixtures.broadcast("t1:news"));
    scenario.produce(r);
    Frame f = c.awaitPush(T);
    assertThat(f.getMsgId()).isEqualTo(r.getMsgId());
    assertThat(f.getTopic()).isEqualTo("t1:news");
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> other.awaitPush(Duration.ofMillis(500)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void controlKickClosesTheOlderSession4012() {
    String p = principal();
    GatewayClient older = client(p, "d1");
    GatewayClient newer = client(p, "d2");
    Connected n = connected.get(newer);
    scenario.produce(fresh(Fixtures.control(p, Fixtures.kick(n.getConnId(), System.currentTimeMillis()))));
    CloseInfo closed = older.awaitClose(T);
    assertThat(closed.code()).isEqualTo(4012);
    assertThat(newer.isOpen()).isTrue();
  }

  @Test
  void stoppingTheGatewayEmitsNodeDown() throws Exception {
    String id = "gw-r2-" + UUID.randomUUID().toString().substring(0, 6);
    ConfigurableApplicationContext gw2 = startGateway(id);
    long epoch = gw2.getBean(NodeIdentity.class).epoch();
    Thread.sleep(3_000);   // the router's directory (refresh 1 s) has seen it live
    gw2.close();
    assertThat(scenario.awaitNodeDown(id, epoch).getNodeId()).isEqualTo(id);
  }

  @Test
  void restartingTheRouterRedeliversUnfinishedRecords() throws Exception {
    router.close();
    router = startRouter("60s");
    try {
      String p = principal();
      GatewayClient c = client(p, "d1");
      PushRecord r = fresh(Fixtures.message(p)).toBuilder().setReceipt(true).setSenderPrincipalId("t1:svc").build();
      scenario.produce(r);
      assertThat(c.awaitPush(T).getMsgId()).isEqualTo(r.getMsgId());   // withheld: the record stays unfinished
      router.close();
      router = startRouter("2s");
      // The gateway still holds the pending entry for this msg_id: the restarted router's delivery replaces its
      // delivery_id (the client is not written to twice), and the late ACK completes the redelivered record.
      Thread.sleep(2_000);
      c.ack(r.getMsgId());
      assertThat(scenario.awaitDelivered(r.getMsgId()).getStatus()).isEqualTo(AckItem.Status.STATUS_ACKED);
    } finally {
      if (!router.isActive()) {
        router = startRouter("2s");
      }
    }
  }

  @Test
  void twentyMessagesToOnePrincipalArriveInOrder() {
    String p = principal();
    GatewayClient c = client(p, "d1");
    List<String> got = new CopyOnWriteArrayList<>();
    c.onPush(f -> {
      got.add(f.getMsgId());
      c.ack(f.getMsgId());
    });
    List<String> sent = new ArrayList<>();
    for (int i = 1; i <= 20; i++) {
      PushRecord r = fresh(Fixtures.message(p));
      r = r.toBuilder().setFrame(r.getFrame().toBuilder().setSeq(i)).build();
      sent.add(r.getMsgId());
      scenario.produce(r);
    }
    await().atMost(T).until(() -> got.size() >= 20);
    assertThat(got).containsExactlyElementsOf(sent);
  }
}
