package com.pangate.devtools.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.FrameType;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class GatewayClientTest {

  private static final Duration T = Duration.ofSeconds(5);
  private static FakeGatewayServer server;

  @BeforeAll
  static void start() throws Exception {
    server = new FakeGatewayServer();
  }

  @AfterAll
  static void stop() {
    server.close();
  }

  private static URI ws() {
    return URI.create("wss://localhost:" + server.wsPort() + "/v1/ws");
  }

  @Test
  void helloReturnsConnected() {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      assertThat(c.hello(Hellos.jwt("x", "d1"), T).getConnId()).isEqualTo("01TEST");
    }
  }

  @Test
  void trustedCaWithHostnameVerification() {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.trust(server.cert.certificate().toPath()), T)) {
      assertThat(c.hello(Hellos.jwt("x", "d1"), T).getConnId()).isEqualTo("01TEST");
    }
  }

  @Test
  void pingReturnsPong() {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      c.hello(Hellos.jwt("x", "d1"), T);
      assertThat(c.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
    }
  }

  @Test
  void serverCloseSurfacesCode() {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      assertThatThrownBy(() -> c.hello(Hellos.jwt("x", "kick"), T))
          .isInstanceOfSatisfying(GatewayClosedException.class, e -> {
            assertThat(e.closeInfo().code()).isEqualTo(4012);
            assertThat(e.closeInfo().reason()).isEqualTo("DUPLICATE_SESSION");
          });
    }
  }

  @Test
  void tcpTransportWorks() {
    URI tcp = URI.create("tcps://localhost:" + server.tcpPort());
    try (GatewayClient c = GatewayClient.connect(tcp, ClientTls.insecure(), T)) {
      assertThat(c.hello(Hellos.ticket("tk", "d1"), T).getConnId()).isEqualTo("01TEST");
      assertThat(c.ping(T).getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
    }
  }

  @Test
  void tcpFatalErrorSurfacesCode() {
    URI tcp = URI.create("tcps://localhost:" + server.tcpPort());
    try (GatewayClient c = GatewayClient.connect(tcp, ClientTls.insecure(), T)) {
      assertThatThrownBy(() -> c.hello(Hellos.jwt("x", "kick"), T))
          .isInstanceOfSatisfying(GatewayClosedException.class, e -> assertThat(e.closeInfo().code()).isEqualTo(4012));
    }
  }
  @Test
  void upstreamReturnsAck() {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      c.hello(Hellos.jwt("t", "d1"), T);

      com.pangate.contracts.v1.Frame ack = c.upstream("cmd", "m-9", "x".getBytes(), T);

      assertThat(ack.getType()).isEqualTo(com.pangate.contracts.v1.FrameType.FRAME_TYPE_UPSTREAM_ACK);
      assertThat(ack.getMsgId()).isEqualTo("m-9");
    }
  }

  @Test
  void upstreamReturnsErrorFrame() {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      c.hello(Hellos.jwt("t", "d1"), T);

      assertThat(c.upstream("nope", "m-1", new byte[0], T).getType())
          .isEqualTo(com.pangate.contracts.v1.FrameType.FRAME_TYPE_ERROR);
    }
  }

  @Test
  void subscribeSendsSubscribePayload() throws Exception {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      c.hello(Hellos.jwt("x", "d1"), T);
      c.subscribe("t1:a", "t1:b");
      com.pangate.contracts.v1.Frame push = c.awaitPush(T);
      com.pangate.contracts.v1.Frame sub = server.received.stream()
          .filter(f -> f.getType() == FrameType.FRAME_TYPE_SUBSCRIBE).reduce((a, b) -> b).orElseThrow();
      assertThat(com.pangate.contracts.v1.Subscribe.parseFrom(sub.getPayload()).getTopicsList())
          .containsExactly("t1:a", "t1:b");
      assertThat(sub.getMsgId()).isNotEmpty();
      assertThat(push.getTopic()).isEqualTo("t1:a");
    }
  }

  @Test
  void awaitPushAndAckSendsAckFrame() throws Exception {
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      c.hello(Hellos.jwt("x", "d1"), T);
      c.subscribe("t1:x");
      assertThat(c.awaitPush(T).getMsgId()).isEqualTo("p1");
      c.ack("p1");
      c.ping(T);  // ordered after the ACK on the same connection
      com.pangate.contracts.v1.Frame ack = server.received.stream()
          .filter(f -> f.getType() == FrameType.FRAME_TYPE_ACK).reduce((a, b) -> b).orElseThrow();
      assertThat(com.pangate.contracts.v1.Ack.parseFrom(ack.getPayload()).getMsgIdsList()).containsExactly("p1");
    }
  }

  @Test
  void pushListenerReceivesPushesInsteadOfTheQueue() throws Exception {
    java.util.List<com.pangate.contracts.v1.Frame> got = new java.util.concurrent.CopyOnWriteArrayList<>();
    try (GatewayClient c = GatewayClient.connect(ws(), ClientTls.insecure(), T)) {
      c.onPush(got::add);
      c.hello(Hellos.jwt("x", "d1"), T);
      c.subscribe("t1:x");
      org.awaitility.Awaitility.await().atMost(T).until(() -> got.size() == 1);
    }
  }
}
