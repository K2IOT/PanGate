package com.pangate.gateway.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.CellMap;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.EventType;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Reconnect;
import com.pangate.contracts.v1.Revoke;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.UpstreamRecord;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.gateway.GatewayApplication;
import com.pangate.gateway.diagnostics.DependencyFailureAnalyzer;
import com.pangate.gateway.diagnostics.DependencyUnavailableException;
import com.pangate.gateway.registry.RedisSeqReader;
import com.pangate.gateway.server.NodeState;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
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
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** M2a end to end: a real gateway (profile local) on Testcontainers Redis and Kafka (spec §11, §12 M2). */
class GatewayClusterIT {

  private static final Duration T = Duration.ofSeconds(10);
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @TempDir static Path devDir;
  private static Gateway gw;
  private static RedisClient redisClient;
  private static RedisCommands<String, String> redis;
  private static KafkaProducer<String, byte[]> producer;

  /** One running gateway: its ports, node id and Spring context. */
  record Gateway(int ws, int tcp, int mgmt, String nodeId, ConfigurableApplicationContext ctx) {

    static Gateway start(String nodeId, String... extra) throws Exception {
      int ws = freePort();
      int tcp = freePort();
      int mgmt = freePort();
      List<String> args = new ArrayList<>(List.of("--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + ws,
          "--gateway.listeners.tcp.port=" + tcp, "--server.port=" + mgmt, "--gateway.io.threads=2", "--gateway.grpc.port=0",
          "--gateway.node-id=" + nodeId));
      args.addAll(InfraContainers.gatewayArgs());
      args.addAll(List.of(extra));
      ConfigurableApplicationContext ctx = new SpringApplicationBuilder(GatewayApplication.class).profiles("local")
          .run(args.toArray(String[]::new));
      Gateway g = new Gateway(ws, tcp, mgmt, nodeId, ctx);
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

    long epoch() {
      return ctx.getBean(NodeState.class).epoch();
    }

    void close() {
      ctx.close();
    }
  }

  static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }

  @BeforeAll
  static void boot() throws Exception {
    redisClient = RedisClient.create(InfraContainers.redisUri());
    redis = redisClient.connect().sync();
    producer = new KafkaProducer<>(Map.of("bootstrap.servers", InfraContainers.kafkaBootstrap()),
        new StringSerializer(), new ByteArraySerializer());
    gw = Gateway.start("gw-it-" + UUID.randomUUID().toString().substring(0, 8));
  }

  @AfterAll
  static void shutdown() {
    if (gw != null) {
      gw.close();
    }
    producer.close();
    redisClient.shutdown();
  }

  private static String principal(String tenant) {
    return tenant + ":it-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static String token(String tenant, String principal, String device) {
    return new JwtMinter(DevKeys.load(devDir.resolve("es256.pem")), "it")
        .mint(new TokenRequest(tenant, principal, device, List.of(), Duration.ofMinutes(10)), Instant.now());
  }

  private static GatewayClient client(Gateway g) {
    return GatewayClient.connect(g.wss(), ClientTls.trust(devDir.resolve("ca.crt")), T);
  }

  private static Connected hello(GatewayClient c, String tenant, String principal) {
    return c.hello(Hellos.jwt(token(tenant, principal, "d1"), "d1"), T);
  }

  /** HELLO that must be refused: returns the close code. */
  private static int refusedHello(Gateway g, String tenant, String principal) {
    try (GatewayClient c = client(g)) {
      c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO)
          .setPayload(Hellos.jwt(token(tenant, principal, "d1"), "d1").toByteString()).build());
      return c.awaitClose(T).code();
    }
  }

  private static void control(String key, ControlEvent e) throws Exception {
    producer.send(new ProducerRecord<>("gw.control", key, e == null ? null : e.toByteArray())).get();
  }

  /** Consumer positioned at the end of every partition of {@code topic}: only records produced afterwards. */
  private static KafkaConsumer<String, byte[]> tail(String topic) {
    KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of("bootstrap.servers",
        InfraContainers.kafkaBootstrap(), "group.id", "it-" + UUID.randomUUID()), new StringDeserializer(),
        new ByteArrayDeserializer());
    List<TopicPartition> parts = c.partitionsFor(topic).stream()
        .map(p -> new TopicPartition(topic, p.partition())).toList();
    c.assign(parts);
    c.seekToEnd(parts);
    parts.forEach(c::position);
    return c;
  }

  private static List<ConsumerRecord<String, byte[]>> poll(KafkaConsumer<String, byte[]> c, String key, int n) {
    List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
    await().atMost(T).until(() -> {
      c.poll(Duration.ofMillis(200)).forEach(r -> {
        if (key.equals(r.key())) {
          out.add(r);
        }
      });
      return out.size() >= n;
    });
    return out;
  }

  @Test
  void readyReportsLeaseAndControl() throws Exception {
    assertThat(gw.status("/ready")).isEqualTo(200);
    assertThat(gw.epoch()).isPositive();
    assertThat(gw.body("/actuator/prometheus")).contains("gw_node_epoch").contains("gw_revocations");
  }

  @Test
  void registryEntryWrittenOnHelloAndRemovedOnClose() {
    String p = principal("t1");
    String connId;
    try (GatewayClient c = client(gw)) {
      connId = hello(c, "t1", p).getConnId();

      await().atMost(T).until(() -> redis.hget("reg:{" + p + "}", connId) != null);
      assertThat(redis.hget("reg:{" + p + "}", connId)).startsWith(gw.nodeId() + "|" + gw.epoch() + "|")
          .endsWith("|iot");
    }

    await().atMost(T).until(() -> !redis.hexists("reg:{" + p + "}", connId));
  }

  @Test
  void connectedCarriesSeqAndSeqEpochFromRedis() {
    String p = principal("t1");
    redis.set("seq:{" + p + "}", "41");
    redis.hset("seqepochs", Integer.toString(SlotHash.getSlot(p)), "3");
    gw.ctx().getBean(RedisSeqReader.class).refreshEpochs();
    await().atMost(T).until(() -> gw.ctx().getBean(RedisSeqReader.class).seqEpoch(p) == 3);

    try (GatewayClient c = client(gw)) {
      Connected connected = hello(c, "t1", p);

      assertThat(connected.getSeq()).isEqualTo(41);
      assertThat(connected.getSeqEpoch()).isEqualTo(3);
      assertThat(connected.getSeqUnknown()).isFalse();
    }
  }

  @Test
  void lifecycleConnectedAndDisconnectedPublished() throws Exception {
    String p = principal("t1");
    try (KafkaConsumer<String, byte[]> lifecycle = tail("c1.gw.lifecycle")) {
      String connId;
      try (GatewayClient c = client(gw)) {
        connId = hello(c, "t1", p).getConnId();
      }

      List<ConsumerRecord<String, byte[]>> events = poll(lifecycle, p, 2);
      ConnectionEvent connected = ConnectionEvent.parseFrom(events.get(0).value());
      ConnectionEvent disconnected = ConnectionEvent.parseFrom(events.get(1).value());
      assertThat(connected.getType()).isEqualTo(EventType.EVENT_TYPE_CONNECTED);
      assertThat(connected.getConnId()).isEqualTo(connId);
      assertThat(connected.getEpoch()).isEqualTo(gw.epoch());
      assertThat(disconnected.getType()).isEqualTo(EventType.EVENT_TYPE_DISCONNECTED);
      assertThat(disconnected.getConnId()).isEqualTo(connId);
      assertThat(disconnected.getCloseCode()).isEqualTo(1000);
    }
  }

  @Test
  void upstreamCmdProducesRecordAndUpstreamAck() throws Exception {
    String p = principal("t1");
    try (KafkaConsumer<String, byte[]> cmd = tail("c1.gw.upstream.cmd"); GatewayClient c = client(gw)) {
      String connId = hello(c, "t1", p).getConnId();

      c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId("m-1").putHeaders("type", "cmd")
          .setPayload(ByteString.copyFromUtf8("turn-on")).build());
      Frame ack = c.await(FrameType.FRAME_TYPE_UPSTREAM_ACK, T);

      assertThat(ack.getMsgId()).isEqualTo("m-1");
      ConsumerRecord<String, byte[]> r = poll(cmd, p, 1).get(0);
      UpstreamRecord rec = UpstreamRecord.parseFrom(r.value());
      assertThat(rec.getConnId()).isEqualTo(connId);
      assertThat(rec.getPayload().toStringUtf8()).isEqualTo("turn-on");
      assertThat(new String(r.headers().lastHeader("tenant-id").value())).isEqualTo("t1");
    }
  }

  @Test
  void unknownUpstreamTypeGetsError4000AndStaysOpen() throws Exception {
    try (GatewayClient c = client(gw)) {
      hello(c, "t1", principal("t1"));

      c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId("m-1").putHeaders("type", "nope")
          .build());
      ErrorInfo e = ErrorInfo.parseFrom(c.await(FrameType.FRAME_TYPE_ERROR, T).getPayload());

      assertThat(e.getCode()).isEqualTo(4000);
      assertThat(c.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
    }
  }

  @Test
  void revokePrincipalClosesSessionWithin10sAndBlocksHello() throws Exception {
    String p = principal("t1");
    try (GatewayClient c = client(gw)) {
      hello(c, "t1", p);
      long t0 = System.nanoTime();

      control("revoke:p:" + p, ControlEvent.newBuilder().setRevoke(Revoke.newBuilder().setPrincipalId(p)
          .setExpiresAtMs(System.currentTimeMillis() + 600_000)).build());

      assertThat(c.awaitClose(T).code()).isEqualTo(4003);
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(10));
    }
    assertThat(refusedHello(gw, "t1", p)).isEqualTo(4003);
  }

  @Test
  void tenantBlockedClosesOpen4013AndRejectsHello4009() throws Exception {
    String tenant = "tblk" + UUID.randomUUID().toString().substring(0, 6);
    String p = principal(tenant);
    try (GatewayClient c = client(gw)) {
      hello(c, tenant, p);

      control("tenantcfg:tenant:" + tenant, ControlEvent.newBuilder().setTenantCfg(TenantCfg.newBuilder()
          .setScope(Scope.SCOPE_TENANT).setScopeId(tenant)
          .setSwitches(KillSwitches.newBuilder().setTenantBlocked(true))).build());

      var close = c.awaitClose(T);
      assertThat(close.code()).isEqualTo(4013);
      assertThat(close.reason()).contains("retryAfter=300000");
    }
    assertThat(refusedHello(gw, tenant, principal(tenant))).isEqualTo(4009);
  }

  @Test
  void cellMapMoveSendsReconnectReBootstrap() throws Exception {
    String tenant = "tmig" + UUID.randomUUID().toString().substring(0, 6);
    try (GatewayClient c = client(gw)) {
      hello(c, tenant, principal(tenant));

      control("cellmap:" + tenant, ControlEvent.newBuilder().setCellMap(CellMap.newBuilder().setTenantId(tenant)
          .setCell("cell-2")).build());

      Reconnect r = Reconnect.parseFrom(c.await(FrameType.FRAME_TYPE_RECONNECT, T).getPayload());
      assertThat(r.getReBootstrap()).isTrue();
      assertThat(r.getDelayMs()).isBetween(0, 5_000);
    }
    assertThat(refusedHello(gw, tenant, principal(tenant))).isEqualTo(4011);
  }

  @Test
  void controlCatchUpGatesReadinessAndReplaysRevokes() throws Exception {
    String p = principal("t1");
    control("revoke:p:" + p, ControlEvent.newBuilder().setRevoke(Revoke.newBuilder().setPrincipalId(p)
        .setExpiresAtMs(System.currentTimeMillis() + 600_000)).build());

    Gateway second = Gateway.start("gw-it-replay-" + UUID.randomUUID().toString().substring(0, 6));
    try {
      assertThat(refusedHello(second, "t1", p)).isEqualTo(4003);
    } finally {
      second.close();
    }
  }

  @Test
  void leaseHasTtl15AndRestartTakesHigherEpoch() throws Exception {
    String node = "gw-it-drill-" + UUID.randomUUID().toString().substring(0, 6);
    Gateway first = Gateway.start(node);
    long epoch = first.epoch();
    assertThat(redis.ttl("node:{" + node + "}")).isBetween(1L, 15L);
    first.close();

    Gateway again = Gateway.start(node);
    try {
      assertThat(again.epoch()).isEqualTo(epoch + 1);
      assertThat(again.status("/ready")).isEqualTo(200);
    } finally {
      again.close();
    }
  }

  @Test
  void redisPauseKeepsSessionsAndReEpochsOnRecovery() throws Exception {
    var container = InfraContainers.redis();
    String pa = principal("t1");
    String pb = principal("t1");
    long epoch = gw.epoch();
    try (GatewayClient a = client(gw); KafkaConsumer<String, byte[]> lifecycle = tail("c1.gw.lifecycle")) {
      String connA = hello(a, "t1", pa).getConnId();
      container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
      String connB;
      try (GatewayClient b = client(gw)) {
        Connected cb = hello(b, "t1", pb);
        connB = cb.getConnId();
        assertThat(cb.getSeqUnknown()).isTrue();
        await().atMost(Duration.ofSeconds(25)).until(() -> gw.status("/ready") == 503);
        assertThat(gw.body("/ready")).contains("node lease lost");
        assertThat(a.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
      } finally {
        container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
      }

      await().atMost(Duration.ofSeconds(15)).until(() -> gw.epoch() > epoch && gw.status("/ready") == 200);
      long newEpoch = gw.epoch();
      await().atMost(T).until(() -> {
        String v = redis.hget("reg:{" + pa + "}", connA);
        return v != null && v.startsWith(gw.nodeId() + "|" + newEpoch + "|");
      });
      List<ConsumerRecord<String, byte[]>> events = new ArrayList<>();
      await().atMost(T).until(() -> {
        lifecycle.poll(Duration.ofMillis(200)).forEach(r -> {
          if (pa.equals(r.key())) {
            events.add(r);
          }
        });
        for (ConsumerRecord<String, byte[]> r : events) {
          ConnectionEvent e = ConnectionEvent.parseFrom(r.value());
          if (e.getType() == EventType.EVENT_TYPE_CONNECTED && e.getEpoch() == newEpoch) {
            return true;
          }
        }
        return false;
      });
      assertThat(a.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
      assertThat(connB).isNotEmpty();
    }
  }

  @Test
  void startupFailsFastWithDiagnosticWhenKafkaMissing() {
    assertThatThrownBy(() -> new SpringApplicationBuilder(GatewayApplication.class).profiles("local").run(
        "--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + freePort(),
        "--gateway.listeners.tcp.port=" + freePort(), "--server.port=" + freePort(), "--gateway.grpc.port=0",
        "--gateway.node-id=gw-it-nokafka", "--gateway.redis.registry-uri=" + InfraContainers.redisUri(),
        "--gateway.redis.state-uri=" + InfraContainers.redisUri(), "--gateway.kafka.bootstrap=localhost:1",
        "--gateway.startup.dependency-timeout=3s"))
        .satisfies(e -> {
          assertThat(e).hasRootCauseInstanceOf(Exception.class);
          Throwable t = e;
          while (t != null && !(t instanceof DependencyUnavailableException)) {
            t = t.getCause();
          }
          assertThat(t).isInstanceOf(DependencyUnavailableException.class);
          assertThat(new DependencyFailureAnalyzer().analyze(e).getDescription())
              .isEqualTo("Kafka localhost:1 is not reachable after 3 s");
        });
  }
}
