package com.pangate.gateway.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Reconnect;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.devtools.router.FakeRouter;
import com.pangate.gateway.GatewayApplication;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.downstream.GrpcServerLifecycle;
import com.pangate.gateway.server.NodeState;
import io.lettuce.core.RedisClient;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** M3a end to end (spec §11, §12 M3): warm-up, drain, slow consumer, shed ladder, recycling, admission kill switch. */
class GatewayProtectionIT {

  private static final Duration T = Duration.ofSeconds(10);
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @TempDir static Path devDir;
  private static KafkaProducer<String, byte[]> producer;
  private Gw gw;
  private FakeRouter router;
  private final List<GatewayClient> clients = new ArrayList<>();
  private final Map<GatewayClient, String> connIds = new java.util.IdentityHashMap<>();

  record Gw(int ws, int mgmt, String nodeId, ConfigurableApplicationContext ctx) {

    static Gw start(String... extra) throws Exception {
      int ws = GatewayClusterIT.freePort();
      int mgmt = GatewayClusterIT.freePort();
      String nodeId = "gw-prot-" + UUID.randomUUID().toString().substring(0, 8);
      List<String> args = new ArrayList<>(List.of("--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + ws,
          "--gateway.listeners.tcp.port=" + GatewayClusterIT.freePort(), "--server.port=" + mgmt,
          "--gateway.io.threads=2", "--gateway.grpc.port=0", "--gateway.node-id=" + nodeId));
      // A key given twice on the command line binds as "a,b": add the default only when the test does not set it.
      if (java.util.Arrays.stream(extra).noneMatch(a -> a.startsWith("--gateway.warmup.iterations="))) {
        args.add("--gateway.warmup.iterations=0");
      }
      args.addAll(InfraContainers.gatewayArgs());
      args.addAll(List.of(extra));
      ConfigurableApplicationContext ctx = new SpringApplicationBuilder(GatewayApplication.class).profiles("local")
          .run(args.toArray(String[]::new));
      Gw g = new Gw(ws, mgmt, nodeId, ctx);
      await().atMost(Duration.ofSeconds(30)).until(() -> g.status("/ready") == 200);
      return g;
    }

    URI wss() {
      return URI.create("wss://localhost:" + ws + "/v1/ws");
    }

    int status(String path) throws Exception {
      return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + mgmt + path)).build(),
          HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    String body(String path) throws Exception {
      return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + mgmt + path)).build(),
          HttpResponse.BodyHandlers.ofString()).body();
    }

    int post(String path) throws Exception {
      return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + mgmt + path))
          .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    /** Sum of a Prometheus metric's samples whose labels contain {@code label}. */
    double metric(String name, String label) throws Exception {
      Matcher m = Pattern.compile("(?m)^" + name + "\\{([^}]*)\\} (\\S+)$").matcher(body("/actuator/prometheus"));
      double sum = 0;
      while (m.find()) {
        if (label == null || m.group(1).contains(label)) {
          sum += Double.parseDouble(m.group(2));
        }
      }
      return sum;
    }

    String grpc() {
      return "localhost:" + ctx.getBean(GrpcServerLifecycle.class).port();
    }
  }

  @BeforeAll
  static void boot() {
    producer = new KafkaProducer<>(Map.of("bootstrap.servers", InfraContainers.kafkaBootstrap()),
        new StringSerializer(), new ByteArraySerializer());
  }

  @AfterAll
  static void shutdown() {
    producer.close();
  }

  @AfterEach
  void stop() {
    for (GatewayClient c : clients) {
      try {
        c.abort();
      } catch (RuntimeException ignored) {
        // already closed by the gateway
      }
    }
    if (router != null) {
      router.close();
    }
    if (gw != null) {
      gw.ctx().close();
    }
  }

  private GatewayClient client(String tenant) {
    GatewayClient c = GatewayClient.connect(gw.wss(), ClientTls.trust(devDir.resolve("ca.crt")), T);
    clients.add(c);
    String principal = tenant + ":prot-" + UUID.randomUUID().toString().substring(0, 8);
    String jwt = new JwtMinter(DevKeys.load(devDir.resolve("es256.pem")), "it")
        .mint(new TokenRequest(tenant, principal, "d1", List.of(), Duration.ofMinutes(10)), Instant.now());
    connIds.put(c, c.hello(Hellos.jwt(jwt, "d1"), T).getConnId());
    return c;
  }

  private String connId(GatewayClient c) {
    return connIds.get(c);
  }

  private static void control(String key, ControlEvent e) throws Exception {
    producer.send(new ProducerRecord<>("gw.control", key, e == null ? null : e.toByteArray())).get();
  }

  @Test
  void warmUpRunsBeforeReadyAndLeavesNoTrace() throws Exception {
    try (KafkaConsumer<String, byte[]> lifecycle = new KafkaConsumer<>(Map.of("bootstrap.servers",
        InfraContainers.kafkaBootstrap(), "group.id", "it-" + UUID.randomUUID()), new StringDeserializer(),
        new ByteArrayDeserializer())) {
      List<TopicPartition> parts = lifecycle.partitionsFor("c1.gw.lifecycle").stream()
          .map(p -> new TopicPartition("c1.gw.lifecycle", p.partition())).toList();
      lifecycle.assign(parts);
      lifecycle.seekToEnd(parts);
      parts.forEach(lifecycle::position);

      gw = Gw.start("--gateway.warmup.iterations=20");

      assertThat(gw.metric("gw_handshakes_total", "result=\"warmup\"")).isEqualTo(20);
      assertThat(gw.metric("gw_handshakes_total", "result=\"ok\"")).isZero();
      RedisClient redis = RedisClient.create(InfraContainers.redisUri());
      try {
        assertThat(redis.connect().sync().keys("reg:{__warmup*")).isEmpty();
      } finally {
        redis.shutdown();
      }
      long end = System.nanoTime() + Duration.ofSeconds(3).toNanos();
      while (System.nanoTime() < end) {
        lifecycle.poll(Duration.ofMillis(200)).forEach(r -> assertThat(r.key()).doesNotStartWith("__warmup"));
      }
    }
  }

  @Test
  void drainSendsReconnectAndForceClosesStragglers() throws Exception {
    gw = Gw.start("--gateway.drain.interval=1s", "--gateway.drain.batch-percent=50",
        "--gateway.drain.max-delay=500ms", "--gateway.drain.max-duration=6s", "--gateway.drain.listener-close-delay=0s",
        "--gateway.limits.pending-ack.timeout=30s");
    router = FakeRouter.connect(gw.grpc());
    List<GatewayClient> cs = List.of(client("t1"), client("t1"), client("t1"), client("t1"));
    GatewayClient straggler = cs.get(3);
    long pending = router.unicast(connId(straggler), DeliveryClass.DELIVERY_CLASS_RELIABLE, "m-drain", new byte[0]);
    straggler.awaitPush(T);

    assertThat(gw.post("/admin/drain")).isEqualTo(202);
    await().atMost(T).until(() -> gw.status("/ready") == 503);
    assertThat(gw.body("/ready")).contains("draining");
    RedisClient redis = RedisClient.create(InfraContainers.redisUri());
    try {
      await().atMost(T).until(() -> "1".equals(redis.connect().sync().get("node:draining:{" + gw.nodeId() + "}")));
    } finally {
      redis.shutdown();
    }
    for (GatewayClient c : cs) {
      assertThat(Reconnect.parseFrom(c.await(FrameType.FRAME_TYPE_RECONNECT, Duration.ofSeconds(3)).getPayload())
          .getReBootstrap()).isFalse();
    }
    cs.subList(0, 3).forEach(GatewayClient::close);
    assertThat(straggler.awaitClose(Duration.ofSeconds(8)).code()).isEqualTo(1001);
    assertThat(router.awaitAck(pending, T).getStatus()).isEqualTo(AckItem.Status.STATUS_NACK_CLOSED);
    assertThatThrownBy(() -> GatewayClient.connect(gw.wss(), ClientTls.trust(devDir.resolve("ca.crt")),
        Duration.ofSeconds(2))).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shutdownAfterAdminDrainWaitsForThatSameDrain() throws Exception {
    gw = Gw.start("--gateway.drain.interval=1s", "--gateway.drain.max-delay=0s", "--gateway.drain.max-duration=6s",
        "--gateway.drain.listener-close-delay=0s");
    GatewayClient straggler = client("t1");  // ignores RECONNECT: stays until the drain's max-duration
    assertThat(gw.post("/admin/drain")).isEqualTo(202);
    straggler.await(FrameType.FRAME_TYPE_RECONNECT, T);
    Thread.sleep(3000);

    long t0 = System.nanoTime();
    gw.ctx().close();  // what SIGTERM does
    long tookMs = (System.nanoTime() - t0) / 1_000_000;

    // A fresh drain would take the full 6 s again; the shared one has only ~3 s left.
    assertThat(tookMs).isBetween(1500L, 5500L);
    assertThat(straggler.awaitClose(T).code()).isEqualTo(1001);
    RedisClient redis = RedisClient.create(InfraContainers.redisUri());
    try {
      assertThat(redis.connect().sync().exists("node:draining:{" + gw.nodeId() + "}")).isZero();
    } finally {
      redis.shutdown();
    }
  }

  @Test
  void slowConsumerIsClosedWith4008() throws Exception {
    gw = Gw.start("--gateway.limits.slow-consumer-timeout=3s", "--gateway.limits.pending-ack.timeout=30s");
    router = FakeRouter.connect(gw.grpc());
    GatewayClient c = client("t1");
    c.pauseReading();
    long pending = router.unicast(connId(c), DeliveryClass.DELIVERY_CLASS_RELIABLE, "m-slow", new byte[0]);
    // Keep pushing until the kernel buffers (auto-tuned, several MB) are full and the channel stays unwritable.
    Thread pusher = Thread.ofVirtual().start(() -> {
      byte[] big = new byte[60_000];
      for (int i = 0; i < 5_000 && !Thread.currentThread().isInterrupted(); i++) {
        router.unicast(connId(c), DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "e" + i, big);
        try {
          Thread.sleep(2);
        } catch (InterruptedException e) {
          return;
        }
      }
    });
    try {
      assertThat(router.awaitAck(pending, Duration.ofSeconds(20)).getStatus())
          .isEqualTo(AckItem.Status.STATUS_NACK_SLOW_CONSUMER);
    } finally {
      pusher.interrupt();
    }
    assertThat(gw.metric("gw_slow_consumer_closes_total", "reason=\"unwritable\"")).isEqualTo(1);
  }

  @Test
  void outboundPressureRaisesShedLevelAndRelievesTheNode() throws Exception {
    gw = Gw.start("--gateway.outbound-budget=256KB", "--gateway.limits.slow-consumer-timeout=5m");
    router = FakeRouter.connect(gw.grpc());
    NodeState node = gw.ctx().getBean(NodeState.class);
    com.pangate.gateway.shed.OutboundBudget budget = gw.ctx().getBean(com.pangate.gateway.shed.OutboundBudget.class);
    List<GatewayClient> paused = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      GatewayClient c = client("t1");
      c.pauseReading();
      paused.add(c);
    }
    GatewayClient reader = client("t1");
    // Sustained pressure: keep pushing until the kernel buffers of the paused clients are full, so Netty's outbound
    // buffers stay above the budget long enough for level 5 to relieve the node.
    Thread pusher = Thread.ofVirtual().start(() -> {
      byte[] big = new byte[60_000];
      for (int i = 0; !Thread.currentThread().isInterrupted(); i++) {
        router.unicast(connId(paused.get(i % paused.size())), DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "e" + i, big);
        if (i % paused.size() == 0) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    double maxBudget = 0;
    int maxLevel = 0;
    try {
      long end = System.nanoTime() + Duration.ofSeconds(40).toNanos();
      while (System.nanoTime() < end && gw.metric("gw_slow_consumer_closes_total", "reason=\"shed\"") < 1) {
        maxBudget = Math.max(maxBudget, budget.ratio());
        maxLevel = Math.max(maxLevel, node.shedLevel());
        Thread.sleep(20);
      }
    } finally {
      pusher.interrupt();
    }
    String seen = "max budget ratio " + maxBudget + ", max level seen " + maxLevel + ", dropped unwritable "
        + gw.metric("gw_push_dropped_total", "reason=\"unwritable\"");
    // A SHED close proves level 5: the sampler relieves the node only in a sample that evaluated L5
    // (ShedSamplerTest). Polling node.shedLevel() here instead missed it on a slow runner: L5 lasts one 100 ms
    // sample, the relief closes the paused clients, and the next sample can already be back at L1.
    assertThat(gw.metric("gw_slow_consumer_closes_total", "reason=\"shed\"")).as(seen).isGreaterThanOrEqualTo(1);
    assertThat(gw.metric("gw_closes_total", "code=\"4008\"")).isGreaterThanOrEqualTo(1);
    reader.ping(T);
    await().atMost(Duration.ofSeconds(5)).until(() -> node.shedLevel() <= 1);
  }

  @Test
  void recyclingSendsReconnectWhenDueUnlessTenantDisabled() throws Exception {
    gw = Gw.start("--gateway.recycling.min=2s", "--gateway.recycling.max=3s");
    control("tenantcfg:tenant:t2", ControlEvent.newBuilder().setTenantCfg(TenantCfg.newBuilder()
        .setScope(Scope.SCOPE_TENANT).setScopeId("t2")
        .setSwitches(KillSwitches.newBuilder().setRecycleEnabled(false))).build());
    await().atMost(T).until(() -> !gw.ctx().getBean(TenantLimitsStore.class).policy("t2").recycleEnabled());
    GatewayClient t1 = client("t1");
    GatewayClient t2 = client("t2");
    Frame f = t1.await(FrameType.FRAME_TYPE_RECONNECT, Duration.ofSeconds(5));
    assertThat(Reconnect.parseFrom(f.getPayload()).getReBootstrap()).isFalse();
    assertThatThrownBy(() -> t2.await(FrameType.FRAME_TYPE_RECONNECT, Duration.ofSeconds(6)))
        .isInstanceOf(IllegalStateException.class);
    control("tenantcfg:tenant:t2", null);
  }

  @Test
  void admissionKillSwitchAppliesWithinOneSecond() throws Exception {
    gw = Gw.start("--gateway.admission.burst=2");
    control("tenantcfg:fleet", ControlEvent.newBuilder().setTenantCfg(TenantCfg.newBuilder()
        .setScope(Scope.SCOPE_FLEET).setSwitches(KillSwitches.newBuilder().setAdmissionRatePerNode(1))).build());
    await().atMost(T).until(() -> gw.ctx().getBean(TenantLimitsStore.class).policy(null).admissionRatePerNode() == 1);
    Thread.sleep(1500);  // applied by the 1 s task, and the bucket drained to its new rate
    int refused = 0;
    for (int i = 0; i < 5; i++) {
      try {
        GatewayClient c = GatewayClient.connect(gw.wss(), ClientTls.trust(devDir.resolve("ca.crt")),
            Duration.ofSeconds(2));
        clients.add(c);
      } catch (IllegalStateException e) {
        refused++;
      }
    }
    assertThat(refused).isGreaterThanOrEqualTo(3);
    control("tenantcfg:fleet", null);
    await().atMost(T).until(() -> gw.ctx().getBean(TenantLimitsStore.class).policy(null).admissionRatePerNode() == 0);
    Thread.sleep(1500);
    clients.add(GatewayClient.connect(gw.wss(), ClientTls.trust(devDir.resolve("ca.crt")), T));
  }
}
