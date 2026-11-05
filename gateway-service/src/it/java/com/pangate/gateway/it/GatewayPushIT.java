package com.pangate.gateway.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.EventType;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushStreamGrpc;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.contracts.v1.Topics;
import com.pangate.contracts.v1.UpstreamRecord;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.devtools.router.FakePushApi;
import com.pangate.devtools.router.FakeRouter;
import com.pangate.gateway.GatewayApplication;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.downstream.GrpcServerLifecycle;
import com.pangate.gateway.session.ConnId;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
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
import java.util.concurrent.atomic.AtomicReference;
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

/**
 * M2b end to end (spec §11, §12 M2): two real gateways on Testcontainers Redis and Kafka, the devtools fake router
 * and fake Push API. Gateway B sends KICK through the fake Push API, which routes it to gateway A.
 */
class GatewayPushIT {

  private static final Duration T = Duration.ofSeconds(10);
  private static final HttpClient HTTP = HttpClient.newHttpClient();
  private static final String SINGLE = "tss";  // single-session tenant

  @TempDir static Path devDir;
  private static Gw a;
  private static Gw b;
  private static FakePushApi pushApi;
  private static FakeRouter router;
  private static KafkaProducer<String, byte[]> producer;

  record Gw(int ws, int mgmt, ConfigurableApplicationContext ctx) {

    static Gw start(String nodeId, String... extra) throws Exception {
      int ws = GatewayClusterIT.freePort();
      int mgmt = GatewayClusterIT.freePort();
      List<String> args = new ArrayList<>(List.of("--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + ws,
          "--gateway.listeners.tcp.port=" + GatewayClusterIT.freePort(), "--server.port=" + mgmt,
          "--gateway.io.threads=2", "--gateway.grpc.port=0", "--gateway.node-id=" + nodeId));
      args.addAll(InfraContainers.gatewayArgs());
      args.addAll(List.of(extra));
      ConfigurableApplicationContext ctx = new SpringApplicationBuilder(GatewayApplication.class).profiles("local")
          .run(args.toArray(String[]::new));
      Gw g = new Gw(ws, mgmt, ctx);
      await().atMost(Duration.ofSeconds(30)).until(() -> HTTP.send(HttpRequest.newBuilder(
          URI.create("http://127.0.0.1:" + mgmt + "/ready")).build(), HttpResponse.BodyHandlers.discarding())
          .statusCode() == 200);
      return g;
    }

    URI wss() {
      return URI.create("wss://localhost:" + ws + "/v1/ws");
    }

    String grpc() {
      return "localhost:" + ctx.getBean(GrpcServerLifecycle.class).port();
    }
  }

  @BeforeAll
  static void boot() throws Exception {
    producer = new KafkaProducer<>(Map.of("bootstrap.servers", InfraContainers.kafkaBootstrap()),
        new StringSerializer(), new ByteArraySerializer());
    String run = UUID.randomUUID().toString().substring(0, 6);
    a = Gw.start("gw-push-a-" + run);
    pushApi = FakePushApi.start(0, List.of(a.grpc()));
    b = Gw.start("gw-push-b-" + run, "--gateway.push-api.enabled=true",
        "--gateway.push-api.target=localhost:" + pushApi.port(), "--gateway.push-api.plaintext=true");
    router = FakeRouter.connect(a.grpc());
    TenantCfg single = TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId(SINGLE)
        .setLimits(TenantLimits.newBuilder().setSingleSession(true)).build();
    producer.send(new ProducerRecord<>("gw.control", "tenantcfg:tenant:" + SINGLE,
        ControlEvent.newBuilder().setTenantCfg(single).build().toByteArray())).get();
    for (Gw g : List.of(a, b)) {
      await().atMost(T).until(() -> g.ctx().getBean(TenantLimitsStore.class).policy(SINGLE).singleSession());
    }
  }

  @AfterAll
  static void shutdown() {
    if (router != null) {
      router.close();
    }
    for (Gw g : new Gw[] {b, a}) {
      if (g != null) {
        g.ctx().close();
      }
    }
    if (pushApi != null) {
      pushApi.close();
    }
    producer.close();
  }

  private static String principal(String tenant) {
    return tenant + ":push-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static GatewayClient client(Gw g) {
    return GatewayClient.connect(g.wss(), ClientTls.trust(devDir.resolve("ca.crt")), T);
  }

  private static String hello(GatewayClient c, String tenant, String principal, List<String> topics) {
    String jwt = new JwtMinter(DevKeys.load(devDir.resolve("es256.pem")), "it")
        .mint(new TokenRequest(tenant, principal, "d1", topics, Duration.ofMinutes(10)), Instant.now());
    return c.hello(Hellos.jwt(jwt, "d1"), T).getConnId();
  }

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
  void reliablePushAckedEndToEnd() throws Exception {
    try (GatewayClient c = client(a)) {
      String connId = hello(c, "t1", principal("t1"), List.of());
      long id = router.unicast(connId, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m-ok", "hi".getBytes());
      Frame push = c.awaitPush(T);
      assertThat(push.getMsgId()).isEqualTo("m-ok");
      assertThat(push.getPayload().toStringUtf8()).isEqualTo("hi");
      c.ack("m-ok");
      assertThat(router.awaitAck(id, T).getStatus()).isEqualTo(AckItem.Status.STATUS_ACKED);
    }
  }

  @Test
  void unackedPushTimesOutAfter5s() throws Exception {
    try (GatewayClient c = client(a)) {
      String connId = hello(c, "t1", principal("t1"), List.of());
      long t0 = System.nanoTime();
      long id = router.unicast(connId, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m-to", new byte[0]);
      c.awaitPush(T);
      assertThat(router.awaitAck(id, Duration.ofSeconds(8)).getStatus())
          .isEqualTo(AckItem.Status.STATUS_NACK_TIMEOUT);
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isBetween(Duration.ofSeconds(4), Duration.ofSeconds(8));
      c.ping(T);
    }
  }

  @Test
  void ephemeralGetsNoAck() throws Exception {
    try (GatewayClient c = client(a)) {
      String connId = hello(c, "t1", principal("t1"), List.of());
      long id = router.unicast(connId, DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "m-eph", new byte[0]);
      assertThat(c.awaitPush(T).getMsgId()).isEqualTo("m-eph");
      Thread.sleep(1000);
      assertThat(router.acks()).noneMatch(x -> x.getDeliveryId() == id);
    }
  }

  @Test
  void rpcReqDeliveredAndRpcResGoesUpstreamAsCmdResult() throws Exception {
    String p = principal("t1");
    try (KafkaConsumer<String, byte[]> results = tail("c1.gw.upstream.cmd_result");
        GatewayClient c = client(a)) {
      String connId = hello(c, "t1", p, List.of());
      long id = router.unicast(connId, Frame.newBuilder().setType(FrameType.FRAME_TYPE_RPC_REQ).setMsgId("rpc-1")
          .setCorrelationId("corr-1").setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE).build());
      Frame req = c.awaitPush(T);
      assertThat(req.getType()).isEqualTo(FrameType.FRAME_TYPE_RPC_REQ);
      assertThat(req.getCorrelationId()).isEqualTo("corr-1");
      c.ack("rpc-1");
      assertThat(router.awaitAck(id, T).getStatus()).isEqualTo(AckItem.Status.STATUS_ACKED);
      c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_RPC_RES).setMsgId("res-1").setCorrelationId("corr-1")
          .setPayload(ByteString.copyFromUtf8("done")).build());
      UpstreamRecord r = UpstreamRecord.parseFrom(poll(results, p, 1).get(0).value());
      assertThat(r.getMsgId()).isEqualTo("res-1");
      assertThat(r.getConnId()).isEqualTo(connId);
    }
  }

  @Test
  void pushToClosedConnIsNotFound() throws Exception {
    String connId;
    try (GatewayClient c = client(a)) {
      connId = hello(c, "t1", principal("t1"), List.of());
    }
    Thread.sleep(300);
    long id = router.unicast(connId, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m-gone", new byte[0]);
    assertThat(router.awaitAck(id, T).getStatus()).isEqualTo(AckItem.Status.STATUS_NACK_NOT_FOUND);
  }

  @Test
  void closeWithPendingNacksClosed() throws Exception {
    long id;
    try (GatewayClient c = client(a)) {
      String connId = hello(c, "t1", principal("t1"), List.of());
      id = router.unicast(connId, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m-pend", new byte[0]);
      c.awaitPush(T);
    }
    assertThat(router.awaitAck(id, T).getStatus()).isEqualTo(AckItem.Status.STATUS_NACK_CLOSED);
  }

  @Test
  void broadcastReachesSubscribersOnEveryLoop() throws Exception {
    List<GatewayClient> subs = new ArrayList<>();
    GatewayClient other = client(a);
    try {
      boolean[] loops = new boolean[2];
      for (int i = 0; i < 12 && !(loops[0] && loops[1]); i++) {
        GatewayClient c = client(a);
        subs.add(c);
        String connId = hello(c, "t1", principal("t1"), List.of("t1:news"));
        loops[ConnId.loopIdx(connId)] = true;
        c.subscribe("t1:news");
        c.ping(T);  // SUBSCRIBE handled before PONG
      }
      assertThat(loops).containsOnly(true);
      hello(other, "t1", principal("t1"), List.of("t1:news"));
      router.broadcast("t1", "t1:news", PushItem.Priority.PRIORITY_NORMAL, "news".getBytes());
      for (GatewayClient c : subs) {
        Frame f = c.awaitPush(T);
        assertThat(f.getTopic()).isEqualTo("t1:news");
        assertThat(f.getPayload().toStringUtf8()).isEqualTo("news");
      }
      assertThatThrownBy(() -> other.awaitPush(Duration.ofMillis(500))).isInstanceOf(IllegalStateException.class);
    } finally {
      subs.forEach(GatewayClient::close);
      other.close();
    }
  }

  @Test
  void controlSubscribeThenBroadcast() throws Exception {
    String p = principal("t1");
    try (GatewayClient c = client(a)) {
      hello(c, "t1", p, List.of());  // no patterns: only control SUBSCRIBE can add the topic
      router.control("t1", p, ControlOp.newBuilder().setSubscribe(Topics.newBuilder().addTopics("t1:ctl")).build());
      AtomicReference<Frame> got = new AtomicReference<>();
      await().atMost(T).until(() -> {
        router.broadcast("t1", "t1:ctl", PushItem.Priority.PRIORITY_CRITICAL, "x".getBytes());
        try {
          got.set(c.awaitPush(Duration.ofMillis(300)));
          return true;
        } catch (IllegalStateException e) {
          return false;
        }
      });
      assertThat(got.get().getTopic()).isEqualTo("t1:ctl");
    }
  }

  @Test
  void singleSessionKickSameNode() throws Exception {
    String p = principal(SINGLE);
    try (KafkaConsumer<String, byte[]> lifecycle = tail("c1.gw.lifecycle");
        GatewayClient c1 = client(a); GatewayClient c2 = client(a)) {
      String first = hello(c1, SINGLE, p, List.of());
      Thread.sleep(5);
      hello(c2, SINGLE, p, List.of());
      assertThat(c1.awaitClose(T).code()).isEqualTo(4012);
      c2.ping(T);
      List<ConsumerRecord<String, byte[]>> events = poll(lifecycle, p, 3);
      assertThat(events.stream().map(r -> {
        try {
          return ConnectionEvent.parseFrom(r.value());
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }).filter(e -> e.getType() == EventType.EVENT_TYPE_KICKED).map(ConnectionEvent::getConnId)).containsExactly(first);
    }
  }

  @Test
  void singleSessionKickAcrossNodesViaPushApi() throws Exception {
    String p = principal(SINGLE);
    try (GatewayClient c1 = client(a); GatewayClient c2 = client(b)) {
      hello(c1, SINGLE, p, List.of());
      Thread.sleep(5);
      hello(c2, SINGLE, p, List.of());
      long t0 = System.nanoTime();
      assertThat(c1.awaitClose(T).code()).isEqualTo(4012);
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(5));
      assertThat(pushApi.received()).anyMatch(r -> r.getPrincipalId().equals(p));
      c2.ping(T);
    }
  }

  @Test
  void wrongVersionHeaderRejected() throws Exception {
    ManagedChannel ch = NettyChannelBuilder.forTarget(a.grpc()).usePlaintext().build();
    try {
      AtomicReference<Throwable> error = new AtomicReference<>();
      StreamObserver<PushBatch> out = PushStreamGrpc.newStub(ch).stream(new StreamObserver<>() {
        @Override public void onNext(DeliveryAck v) { }
        @Override public void onError(Throwable t) { error.set(t); }
        @Override public void onCompleted() { }
      });
      out.onNext(PushBatch.getDefaultInstance());
      await().atMost(T).until(() -> error.get() != null);
      assertThat(Status.fromThrowable(error.get()).getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    } finally {
      ch.shutdownNow();
    }
  }
}
