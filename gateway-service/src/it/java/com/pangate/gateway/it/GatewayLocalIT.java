package com.pangate.gateway.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import com.pangate.gateway.GatewayApplication;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.shed.IpConnectionCounter;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Boots the real gateway with profile local (dev secrets in a temp dir) and talks to it with the devtools client. */
class GatewayLocalIT {

  private static final Duration T = Duration.ofSeconds(10);
  private static final AtomicInteger PRINCIPALS = new AtomicInteger();

  @TempDir static Path devDir;
  private static Ports ports;
  private static ConfigurableApplicationContext ctx;
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  record Ports(int ws, int tcp, int mgmt) {
    static Ports free() throws Exception {
      return new Ports(freePort(), freePort(), freePort());
    }
  }

  static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }

  static ConfigurableApplicationContext start(Ports p) {
    return new SpringApplicationBuilder(GatewayApplication.class).profiles("local").run(
        "--gateway.dev.dir=" + devDir,
        "--gateway.listeners.ws.port=" + p.ws(),
        "--gateway.listeners.tcp.port=" + p.tcp(),
        "--server.port=" + p.mgmt(),
        "--gateway.io.threads=4");
  }

  private static String SHARED_POOL_ID;

  @BeforeAll
  static void boot() throws Exception {
    ports = Ports.free();
    ctx = start(ports);
    SHARED_POOL_ID = Thread.getAllStackTraces().keySet().stream().map(Thread::getName)
        .filter(n -> n.startsWith("gw-io-")).map(n -> n.split("-")[2]).findFirst().orElse("none");
  }

  @AfterAll
  static void shutdown() {
    if (ctx != null) {
      ctx.close();
    }
  }

  private static URI wss(Ports p) {
    return URI.create("wss://localhost:" + p.ws() + "/v1/ws");
  }

  private static ClientTls tls() {
    return ClientTls.trust(devDir.resolve("ca.crt"));
  }

  private static String principal() {
    return "t1:it-" + PRINCIPALS.incrementAndGet();
  }

  private static String token(String principal, String device) {
    return token(DevKeys.load(devDir.resolve("es256.pem")), principal, device);
  }

  private static String token(ECKey key, String principal, String device) {
    return new JwtMinter(key, "it").mint(new TokenRequest("t1", principal, device, List.of(), Duration.ofMinutes(10)),
        Instant.now());
  }

  private static int get(Ports p, String path) throws Exception {
    return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + p.mgmt() + path)).build(),
        HttpResponse.BodyHandlers.discarding()).statusCode();
  }

  private static String body(Ports p, String path) throws Exception {
    return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + p.mgmt() + path)).build(),
        HttpResponse.BodyHandlers.ofString()).body();
  }

  @Test
  void becomesReadyWithin30s() throws Exception {
    await().atMost(Duration.ofSeconds(30)).until(() -> get(ports, "/ready") == 200);

    assertThat(body(ports, "/ready")).contains("\"status\":\"UP\"");
    assertThat(get(ports, "/live")).isEqualTo(200);
    assertThat(body(ports, "/actuator/prometheus")).contains("gw_connections_active").contains("gw_eventloop_lag");
  }

  @Test
  void jwtHelloThenPingOverWss() {
    try (GatewayClient c = GatewayClient.connect(wss(ports), tls(), T)) {
      Connected connected = c.hello(Hellos.jwt(token(principal(), "d1"), "d1"), T);

      assertThat(connected.getConnId()).hasSize(26);
      assertThat(connected.getSeqUnknown()).isTrue();
      assertThat(connected.getHeartbeatSec()).isEqualTo(30);
      assertThat(c.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
    }
  }

  @Test
  void queryStringInUpgradeUrlIsAccepted() {
    URI withQuery = URI.create("wss://localhost:" + ports.ws() + "/v1/ws?sdk=js&v=1");
    try (GatewayClient c = GatewayClient.connect(withQuery, tls(), T)) {
      assertThat(c.hello(Hellos.jwt(token(principal(), "d1"), "d1"), T).getConnId()).hasSize(26);
    }
  }

  @Test
  void ticketReconnectKeepsExpiry() {
    Connected first;
    try (GatewayClient c = GatewayClient.connect(wss(ports), tls(), T)) {
      first = c.hello(Hellos.jwt(token(principal(), "d1"), "d1"), T);
    }
    try (GatewayClient c = GatewayClient.connect(wss(ports), tls(), T)) {
      Connected second = c.hello(Hellos.ticket(first.getResumeTicket(), "d1"), T);

      assertThat(second.getSessionExpiresAtMs()).isEqualTo(first.getSessionExpiresAtMs());
      assertThat(second.getConnId()).isNotEqualTo(first.getConnId());
    }
  }

  @Test
  void tcpListenerAcceptsHello() {
    try (GatewayClient c = GatewayClient.connect(URI.create("tcps://localhost:" + ports.tcp()), tls(), T)) {
      assertThat(c.hello(Hellos.jwt(token(principal(), "d1"), "d1"), T).getConnId()).hasSize(26);
      assertThat(c.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
    }
  }

  @Test
  void pipelinedHelloAndPingWithRotatedKey() throws Exception {
    ECKey rotated = new ECKeyGenerator(Curve.P_256).keyIDFromThumbprint(true).generate();
    Path jwks = devDir.resolve("jwks.json");
    List<JWK> keys = new java.util.ArrayList<>(JWKSet.load(jwks.toFile()).getKeys());
    keys.add(rotated.toPublicJWK());
    Files.writeString(jwks, new JWKSet(keys).toString(true));
    Files.setLastModifiedTime(jwks, FileTime.from(Instant.now().plusSeconds(5)));

    try (GatewayClient c = GatewayClient.connect(wss(ports), tls(), T)) {
      c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO)
          .setPayload(Hellos.jwt(token(rotated, principal(), "d1"), "d1").toByteString()).build());
      c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).setMsgId("p1").build());

      assertThat(c.await(FrameType.FRAME_TYPE_CONNECTED, T)).isNotNull();
      assertThat(c.await(FrameType.FRAME_TYPE_PONG, T).getMsgId()).isEqualTo("p1");
    }
  }

  @Test
  void abruptDisconnectsLeaveNoTrackedState() throws Exception {
    for (int i = 0; i < 50; i++) {
      try (Socket s = new Socket("127.0.0.1", ports.ws())) {
        s.setSoLinger(true, 0);
      }
    }
    for (int i = 0; i < 5; i++) {
      try (Socket s = new Socket("127.0.0.1", ports.ws())) {
        s.getOutputStream().write("GET / HTTP/1.1\r\n".getBytes(StandardCharsets.US_ASCII));
      }
    }
    for (int i = 0; i < 5; i++) {
      // Mid-upgrade: TLS completes, then a partial HTTP upgrade request, then the client vanishes.
      try (SSLSocket tlsSocket = (SSLSocket) trustingContext().getSocketFactory().createSocket("localhost", ports.ws())) {
        tlsSocket.getOutputStream().write("GET /v1/ws HTTP/1.1\r\nHost: local".getBytes(StandardCharsets.US_ASCII));
        tlsSocket.getOutputStream().flush();
      }
    }
    for (int i = 0; i < 10; i++) {
      GatewayClient c = GatewayClient.connect(wss(ports), tls(), T);
      c.hello(Hellos.jwt(token(principal(), "d" + i), "d" + i), T);
      c.abort();
    }

    LoopRegistry loops = ctx.getBean(LoopRegistry.class);
    IpConnectionCounter ips = ctx.getBean(IpConnectionCounter.class);
    PrincipalIndex principals = ctx.getBean(PrincipalIndex.class);
    await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
      assertThat(loops.totalActive()).isZero();
      assertThat(ips.trackedKeys()).isZero();
      assertThat(principals.principalCount()).isZero();
    });
  }

  @Test
  void wrongSubprotocolGets426() throws Exception {
    try (SSLSocket s = (SSLSocket) trustingContext().getSocketFactory().createSocket("localhost", ports.ws())) {
      OutputStream out = s.getOutputStream();
      out.write(("GET /v1/ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
          + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
          + "Sec-WebSocket-Protocol: chat\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
      out.flush();
      String status = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.US_ASCII)).readLine();

      assertThat(status).startsWith("HTTP/1.1 426");
    }
  }

  @Test
  void busyPortFailsFastAndReleasesEverything() throws Exception {
    Ports p = Ports.free();
    try (ServerSocket busy = new ServerSocket(p.tcp())) {
      Throwable failure = null;
      try {
        start(p).close();
      } catch (Throwable t) {
        failure = t;
      }
      Throwable root = failure;
      while (root != null && !(root instanceof com.pangate.gateway.diagnostics.ListenerBindException)) {
        root = root.getCause();
      }
      assertThat(root).as("startup must fail with ListenerBindException").isNotNull();
    }
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      assertThat(Thread.getAllStackTraces().keySet().stream()
          .filter(t -> t.isAlive() && t.getName().startsWith("gw-io-"))
          .filter(t -> !ctxOwns(t))).isEmpty();
      assertThat(canConnect(p.ws())).isFalse();
    });
  }

  /** gw-io threads of the shared context are expected; they belong to its thread group naming "gw-io-1-*". */
  private static boolean ctxOwns(Thread t) {
    return t.getName().startsWith("gw-io-" + SHARED_POOL_ID + "-");
  }

  private static boolean canConnect(int port) {
    try (Socket s = new Socket()) {
      s.connect(new java.net.InetSocketAddress("127.0.0.1", port), 500);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Test
  void shutdownSendsGoingAway() throws Exception {
    Ports p = Ports.free();
    ConfigurableApplicationContext own = start(p);
    GatewayClient c = GatewayClient.connect(wss(p), tls(), T);
    c.hello(Hellos.jwt(token(principal(), "d1"), "d1"), T);

    own.close();

    assertThat(c.awaitClose(T).code()).isEqualTo(1001);
  }

  @Test
  void ticketSignedWithRotatedOutKeyStillResumesAfterRestart() throws Exception {
    Ports p = Ports.free();
    ConfigurableApplicationContext first = start(p);
    Connected connected;
    try (GatewayClient c = GatewayClient.connect(wss(p), tls(), T)) {
      connected = c.hello(Hellos.jwt(token(principal(), "d1"), "d1"), T);
    } finally {
      first.close();
    }
    Path keys = devDir.resolve("ticket-keys");
    String current = Files.readString(keys.resolve("current"));
    String next = Files.readString(keys.resolve("next"));
    byte[] fresh = new byte[32];
    new java.security.SecureRandom().nextBytes(fresh);
    try {
      Files.writeString(keys.resolve("prev"), current);
      Files.writeString(keys.resolve("current"), next);
      Files.writeString(keys.resolve("next"), "99 " + java.util.Base64.getEncoder().encodeToString(fresh) + "\n");
      ConfigurableApplicationContext second = start(p);
      try (GatewayClient c = GatewayClient.connect(wss(p), tls(), T)) {
        assertThat(c.hello(Hellos.ticket(connected.getResumeTicket(), "d1"), T).getConnId()).hasSize(26);
      } finally {
        second.close();
      }
    } finally {
      Files.writeString(keys.resolve("next"), next);
      Files.writeString(keys.resolve("current"), current);
    }
  }

  @Test
  void restartKeepsTicketsValidAndRebindsImmediately() throws Exception {
    Ports p = Ports.free();
    ConfigurableApplicationContext first = start(p);
    Connected connected;
    try (GatewayClient c = GatewayClient.connect(wss(p), tls(), T)) {
      connected = c.hello(Hellos.jwt(token(principal(), "d1"), "d1"), T);
    }

    long t0 = System.nanoTime();
    first.close();
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(15));

    ConfigurableApplicationContext second = start(p);
    try {
      await().atMost(Duration.ofSeconds(30)).until(() -> get(p, "/ready") == 200);
      try (GatewayClient c = GatewayClient.connect(wss(p), tls(), T)) {
        assertThat(c.hello(Hellos.ticket(connected.getResumeTicket(), "d1"), T).getConnId()).hasSize(26);
      }
    } finally {
      second.close();
    }
  }

  private static SSLContext trustingContext() throws Exception {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    try (InputStream in = Files.newInputStream(devDir.resolve("ca.crt"))) {
      ks.setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
    }
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);
    SSLContext ctx = SSLContext.getInstance("TLSv1.3");
    ctx.init(null, tmf.getTrustManagers(), null);
    return ctx;
  }
}
