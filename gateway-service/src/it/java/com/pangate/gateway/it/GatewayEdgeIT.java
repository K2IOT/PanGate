package com.pangate.gateway.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.contracts.v1.TracePrincipal;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.devtools.router.FakeRouter;
import com.pangate.gateway.GatewayApplication;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.dev.DevSecretsGenerator;
import com.pangate.gateway.downstream.GrpcServerLifecycle;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** M3b end to end (spec §4.1, §4.3, §4.6 i, §7.7, §7.8): TLS reload, SSE, HTTP upstream, PROXY, tenant cap, trace. */
class GatewayEdgeIT {

  private static final Duration T = Duration.ofSeconds(10);

  @TempDir static Path devDir;
  private static KafkaProducer<String, byte[]> producer;
  private ConfigurableApplicationContext ctx;
  private int ws;
  private FakeRouter router;
  private final List<GatewayClient> clients = new ArrayList<>();

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
    if (ctx != null) {
      ctx.close();
    }
  }

  private void start(String... extra) throws Exception {
    ws = GatewayClusterIT.freePort();
    int mgmt = GatewayClusterIT.freePort();
    List<String> args = new ArrayList<>(List.of("--gateway.dev.dir=" + devDir, "--gateway.listeners.ws.port=" + ws,
        "--gateway.listeners.tcp.port=" + GatewayClusterIT.freePort(), "--server.port=" + mgmt,
        "--gateway.io.threads=2", "--gateway.grpc.port=0", "--gateway.warmup.iterations=0",
        "--gateway.node-id=gw-edge-" + UUID.randomUUID().toString().substring(0, 8)));
    args.addAll(InfraContainers.gatewayArgs());
    args.addAll(List.of(extra));
    ctx = new SpringApplicationBuilder(GatewayApplication.class).profiles("local").run(args.toArray(String[]::new));
    HttpClient http = HttpClient.newHttpClient();
    await().atMost(Duration.ofSeconds(30)).until(() -> http.send(HttpRequest.newBuilder(
        URI.create("http://127.0.0.1:" + mgmt + "/ready")).build(), HttpResponse.BodyHandlers.discarding())
        .statusCode() == 200);
  }

  private URI wss() {
    return URI.create("wss://localhost:" + ws + "/v1/ws");
  }

  private String jwt(String principal, String device) {
    String tenant = principal.substring(0, principal.indexOf(':'));
    return new JwtMinter(DevKeys.load(devDir.resolve("es256.pem")), "it")
        .mint(new TokenRequest(tenant, principal, device, List.of(), Duration.ofMinutes(10)), Instant.now());
  }

  private GatewayClient client(String principal, InetSocketAddress proxySource) {
    GatewayClient c = GatewayClient.connect(wss(), ClientTls.trust(devDir.resolve("ca.crt")), T, null, proxySource);
    clients.add(c);
    return c;
  }

  private Connected hello(GatewayClient c, String principal) {
    return c.hello(Hellos.jwt(jwt(principal, "d1"), "d1"), T);
  }

  private static void control(String key, ControlEvent e) throws Exception {
    producer.send(new ProducerRecord<>("gw.control", key, e == null ? null : e.toByteArray())).get();
  }

  private TenantLimitsStore tenants() {
    return ctx.getBean(TenantLimitsStore.class);
  }

  private static SSLContext trustDevCa() throws Exception {
    X509Certificate ca;
    try (InputStream in = Files.newInputStream(devDir.resolve("ca.crt"))) {
      ca = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
    }
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    ks.setCertificateEntry("dev-ca", ca);
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);
    SSLContext ssl = SSLContext.getInstance("TLS");
    ssl.init(null, tmf.getTrustManagers(), null);
    return ssl;
  }

  private HttpClient https() throws Exception {
    return HttpClient.newBuilder().sslContext(trustDevCa()).version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(T).build();
  }

  private BigInteger servedSerial() throws Exception {
    try (SSLSocket s = (SSLSocket) trustDevCa().getSocketFactory().createSocket("localhost", ws)) {
      s.startHandshake();
      return ((X509Certificate) s.getSession().getPeerCertificates()[0]).getSerialNumber();
    }
  }

  @Test
  void certificateReplacedOnDiskIsServedWithoutRestart() throws Exception {
    start("--gateway.tls.reload-interval=1s");
    GatewayClient old = client("t1:tls", null);
    hello(old, "t1:tls");
    BigInteger before = servedSerial();

    DevSecretsGenerator.reissueGatewayCert(devDir);

    await().atMost(T).until(() -> !servedSerial().equals(before));
    assertThat(old.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);  // open connections are untouched
  }

  @Test
  void sseStreamsConnectedAndPushes() throws Exception {
    start();
    control("tenantcfg:tenant:t1", ControlEvent.newBuilder().setTenantCfg(TenantCfg.newBuilder()
        .setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setSseFallbackEnabled(true))).build());
    await().atMost(T).until(() -> tenants().policy("t1").sseFallbackEnabled());
    router = FakeRouter.connect("localhost:" + ctx.getBean(GrpcServerLifecycle.class).port());

    byte[] body = Hello.newBuilder().setJwt(jwt("t1:sse", "d1")).setDeviceId("d1").setSdkVersion("1.0").build()
        .toByteArray();
    HttpResponse<Stream<String>> res = https().send(HttpRequest.newBuilder(
            URI.create("https://localhost:" + ws + "/v1/sse"))
        .header("content-type", "application/x-protobuf").POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .build(), HttpResponse.BodyHandlers.ofLines());
    assertThat(res.statusCode()).isEqualTo(200);
    assertThat(res.headers().firstValue("content-type")).hasValue("text/event-stream");
    BlockingQueue<Frame> events = new LinkedBlockingQueue<>();
    Thread reader = Thread.ofVirtual().start(() -> res.body().filter(l -> l.startsWith("data: ")).forEach(l -> {
      try {
        events.add(Frame.parseFrom(Base64.getDecoder().decode(l.substring(6))));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }));
    try {
      Frame connected = events.poll(10, TimeUnit.SECONDS);
      assertThat(connected).isNotNull();
      assertThat(connected.getType()).isEqualTo(FrameType.FRAME_TYPE_CONNECTED);
      String connId = Connected.parseFrom(connected.getPayload()).getConnId();

      long id = router.unicast(connId, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m-sse", new byte[] {1, 2});
      Frame push = events.poll(10, TimeUnit.SECONDS);
      assertThat(push).isNotNull();
      assertThat(push.getType()).isEqualTo(FrameType.FRAME_TYPE_PUSH);
      assertThat(push.getMsgId()).isEqualTo("m-sse");
      assertThat(router.awaitAck(id, T).getStatus()).isEqualTo(AckItem.Status.STATUS_WRITTEN);  // ruling R1
    } finally {
      reader.interrupt();
    }
  }

  @Test
  void httpUpstreamWithResumeTicketReachesKafka() throws Exception {
    start();
    GatewayClient c = client("t1:http", null);
    String ticket = hello(c, "t1:http").getResumeTicket();
    byte[] frame = Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId("m-http")
        .putHeaders("type", "cmd").setPayload(ByteString.copyFromUtf8("hi")).build().toByteArray();

    try (KafkaConsumer<String, byte[]> cmd = new KafkaConsumer<>(Map.of("bootstrap.servers",
        InfraContainers.kafkaBootstrap(), "group.id", "edge-" + UUID.randomUUID()), new StringDeserializer(),
        new ByteArrayDeserializer())) {
      List<TopicPartition> parts = cmd.partitionsFor("c1.gw.upstream.cmd").stream()
          .map(pi -> new TopicPartition(pi.topic(), pi.partition())).toList();
      cmd.assign(parts);
      cmd.seekToEnd(parts);
      parts.forEach(cmd::position);

      HttpResponse<Void> ok = https().send(post(ticket, frame), HttpResponse.BodyHandlers.discarding());
      assertThat(ok.statusCode()).isEqualTo(202);

      List<ConsumerRecord<String, byte[]>> got = new ArrayList<>();
      await().atMost(T).until(() -> {
        cmd.poll(Duration.ofMillis(200)).forEach(r -> {
          if ("t1:http".equals(r.key())) {
            got.add(r);
          }
        });
        return !got.isEmpty();
      });
      assertThat(com.pangate.contracts.v1.UpstreamRecord.parseFrom(got.get(0).value()).getMsgId())
          .isEqualTo("m-http");
    }

    char[] forged = ticket.toCharArray();
    forged[forged.length / 2] = forged[forged.length / 2] == 'A' ? 'B' : 'A';
    assertThat(https().send(post(new String(forged), frame), HttpResponse.BodyHandlers.discarding()).statusCode())
        .isEqualTo(401);
  }

  private HttpRequest post(String ticket, byte[] frame) {
    return HttpRequest.newBuilder(URI.create("https://localhost:" + ws + "/v1/upstream"))
        .header("content-type", "application/x-protobuf").header("authorization", "Ticket " + ticket)
        .POST(HttpRequest.BodyPublishers.ofByteArray(frame)).build();
  }

  @Test
  void proxyProtocolFeedsPerIpLimit() throws Exception {
    start("--gateway.listeners.proxy-protocol.enabled=true",
        "--gateway.listeners.proxy-protocol.trusted-cidrs=127.0.0.0/8",
        "--gateway.listeners.proxy-protocol.trust-loopback=true", "--gateway.limits.per-ip=1");
    InetSocketAddress a = new InetSocketAddress("203.0.113.7", 5555);

    hello(client("t1:px1", a), "t1:px1");
    assertThatThrownBy(() -> client("t1:px2", a)).isInstanceOf(IllegalStateException.class);  // same real IP
    assertThat(hello(client("t1:px3", new InetSocketAddress("203.0.113.8", 5555)), "t1:px3").getConnId())
        .isNotEmpty();
  }

  @Test
  void tenantCapRejectsAboveMaxConnsWith4013() throws Exception {
    start();
    control("tenantcfg:tenant:t2", ControlEvent.newBuilder().setTenantCfg(TenantCfg.newBuilder()
        .setScope(Scope.SCOPE_TENANT).setScopeId("t2").setLimits(TenantLimits.newBuilder().setMaxConns(2))).build());
    await().atMost(T).until(() -> tenants().policy("t2").maxConns() == 2);

    List<GatewayClient> in = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {  // local quota ceil(2 × 1.1 / 1 node) = 3
      GatewayClient c = client("t2:u" + i, null);
      hello(c, "t2:u" + i);
      in.add(c);
    }
    GatewayClient fourth = client("t2:u4", null);
    fourth.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO)
        .setPayload(Hellos.jwt(jwt("t2:u4", "d1"), "d1").toByteString()).build());
    var closed = fourth.awaitClose(T);  // WS: close frame "TENANT_CAP retryAfter=<ms>"
    assertThat(closed.code()).isEqualTo(4013);
    long retryAfter = Long.parseLong(closed.reason().replaceAll(".*retryAfter=", ""));
    assertThat(retryAfter).isBetween(5_000L, 30_000L);

    in.get(0).close();
    await().atMost(T).untilAsserted(() -> {
      GatewayClient c = client("t2:u5", null);
      assertThat(hello(c, "t2:u5").getConnId()).isNotEmpty();
    });
    control("tenantcfg:tenant:t2", null);
  }

  @Test
  void tracePrincipalLogsOnlyThatPrincipal() throws Exception {
    start();
    Logger log = (Logger) LoggerFactory.getLogger("pangate.trace");
    ListAppender<ILoggingEvent> lines = new ListAppender<>();
    lines.start();
    log.addAppender(lines);
    try {
      control("trace:t1:traced", ControlEvent.newBuilder().setIssuedAtMs(System.currentTimeMillis())
          .setTrace(TracePrincipal.newBuilder().setPrincipalId("t1:traced")).build());
      await().atMost(T).until(() -> {
        GatewayClient probe = client("t1:traced", null);
        hello(probe, "t1:traced");
        probe.ping(T);
        return lines.list.stream().anyMatch(l -> l.getFormattedMessage().contains("in PING"));
      });
      int before = lines.list.size();
      GatewayClient other = client("t1:other", null);
      hello(other, "t1:other");
      other.ping(T);
      Thread.sleep(300);
      assertThat(lines.list.size()).isEqualTo(before);
      assertThat(lines.list).noneMatch(l -> l.getFormattedMessage().contains("t1:"));
    } finally {
      log.detachAppender(lines);
      control("trace:t1:traced", null);
    }
  }
}
