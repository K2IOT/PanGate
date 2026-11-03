package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.config.RedisTopology;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.server.Transport;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.SplittableRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UpgradeGuardTest {

  private final LoopFixture f = new LoopFixture();
  private final NodeState node = new NodeState();
  private Session session;

  static GatewayProperties props(int softCap) {
    return new GatewayProperties("n1", "cell-1",
        new GatewayProperties.Listeners(new GatewayProperties.Ws(8443, "/v1/ws", 4), new GatewayProperties.Tcp(8883, true)),
        new GatewayProperties.Io(0, Transport.AUTO), new GatewayProperties.Tls(Path.of("c"), Path.of("k")),
        new GatewayProperties.Limits(700_000, softCap, 200, 65_536, 8_192, Duration.ofSeconds(5),
            Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100),
        new GatewayProperties.Admission(5000, 10000, 500, Duration.ofMinutes(2)),
        new GatewayProperties.Auth("", Path.of("j"), Duration.ofMinutes(5), "pangate-gw", "", Duration.ofSeconds(30)),
        new GatewayProperties.Dev(false, Path.of("d")), new GatewayProperties.Startup(Duration.ofSeconds(30)),
        new GatewayProperties.Redis(RedisTopology.STANDALONE, "redis://localhost:6379", "redis://localhost:6379",
            Duration.ofSeconds(2)),
        new GatewayProperties.Kafka("localhost:9092", true, java.util.List.of("cmd"), "c1",
            org.springframework.util.unit.DataSize.ofMegabytes(128), org.springframework.util.unit.DataSize.ofMegabytes(128)));
  }

  @BeforeEach
  void admit() {
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
  }

  private void guard(int softCap) {
    f.limits.set(new com.pangate.gateway.config.GatewayLimits(700_000, softCap, 200, 65_536, 8_192,
        Duration.ofSeconds(5), Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100));
    f.channel.pipeline().addLast(new UpgradeGuard(props(softCap), f.limits, f.loops, node, new SplittableRandom(1)));
  }

  private static FullHttpRequest upgrade(String path, String subprotocol) {
    FullHttpRequest r = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
    r.headers().set(HttpHeaderNames.UPGRADE, "websocket").set(HttpHeaderNames.ORIGIN, "https://app.example");
    if (subprotocol != null) {
      r.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, subprotocol);
    }
    return r;
  }

  private FullHttpResponse response() {
    return f.channel.readOutbound();
  }

  @Test
  void wrongPathIs404() {
    guard(550_000);

    f.channel.writeInbound(upgrade("/other", "pangate.v1"));

    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(404);
    r.release();
    assertThat(f.channel.isOpen()).isFalse();
  }

  @Test
  void missingSubprotocolIs426() {
    guard(550_000);

    f.channel.writeInbound(upgrade("/v1/ws", "chat"));

    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(426);
    r.release();
  }

  @Test
  void aboveSoftCapIs503WithRetryAfterBetween5And30() {
    guard(1);

    f.channel.writeInbound(upgrade("/v1/ws", "pangate.v1"));

    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(503);
    assertThat(Integer.parseInt(r.headers().get(HttpHeaderNames.RETRY_AFTER))).isBetween(5, 30);
    r.release();
  }

  @Test
  void softCapFollowsHotReloadedLimits() {
    guard(550_000);
    f.limits.set(new com.pangate.gateway.config.GatewayLimits(700_000, 1, 200, 65_536, 8_192, Duration.ofSeconds(5),
        Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100));

    f.channel.writeInbound(upgrade("/v1/ws", "pangate.v1"));

    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(503);
    r.release();
  }

  @Test
  void drainingIs503() {
    node.draining(true);
    guard(550_000);

    f.channel.writeInbound(upgrade("/v1/ws", "pangate.v1"));

    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(503);
    r.release();
  }

  @Test
  void validUpgradePassesThroughAndRemovesGuard() {
    guard(550_000);

    f.channel.writeInbound(upgrade("/v1/ws?x=1", "foo, pangate.v1"));

    FullHttpRequest passed = f.channel.readInbound();
    assertThat(passed.uri()).isEqualTo("/v1/ws?x=1");
    passed.release();
    assertThat(f.channel.pipeline().get(UpgradeGuard.class)).isNull();
    assertThat(session.origin()).isEqualTo("https://app.example");
  }
}
