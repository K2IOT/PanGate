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
        new GatewayProperties.Listeners(new GatewayProperties.Ws(8443, "/v1/ws", 4), new GatewayProperties.Tcp(8883, true), null),
        new GatewayProperties.Io(0, Transport.AUTO), new GatewayProperties.Tls(Path.of("c"), Path.of("k"), java.time.Duration.ofSeconds(30)),
        new GatewayProperties.Limits(700_000, softCap, 200, 65_536, 8_192, Duration.ofSeconds(5),
            Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100,
            new GatewayProperties.PendingAck(32, org.springframework.util.unit.DataSize.ofKilobytes(64),
                Duration.ofSeconds(5)), Duration.ofSeconds(30)),
        new GatewayProperties.Admission(5000, 10000, 500, Duration.ofMinutes(2)),
        new GatewayProperties.Auth("", Path.of("j"), Duration.ofMinutes(5), "pangate-gw", "", Duration.ofSeconds(30)),
        new GatewayProperties.Dev(false, Path.of("d")), new GatewayProperties.Startup(Duration.ofSeconds(30)),
        new GatewayProperties.Redis(RedisTopology.STANDALONE, "redis://localhost:6379", "redis://localhost:6379",
            Duration.ofSeconds(2)),
        new GatewayProperties.Kafka("localhost:9092", true, java.util.List.of("cmd"), "c1",
            org.springframework.util.unit.DataSize.ofMegabytes(128), org.springframework.util.unit.DataSize.ofMegabytes(128)),
        new GatewayProperties.Grpc(9090, true, java.util.List.of(), new GatewayProperties.GrpcTls(null, null, null)),
        new GatewayProperties.PushApi(false, "", true), new GatewayProperties.Broadcast(10_000),
        org.springframework.util.unit.DataSize.ofGigabytes(4),
        new GatewayProperties.Recycling(Duration.ofMinutes(30), Duration.ofMinutes(90), Duration.ofSeconds(30)),
        new GatewayProperties.Drain(2, Duration.ofSeconds(5), Duration.ofSeconds(60), Duration.ofMinutes(10),
            Duration.ofSeconds(10)),
        new GatewayProperties.Warmup(0, 32, Duration.ofSeconds(60)), new GatewayProperties.Shed(true));
  }

  @BeforeEach
  void admit() {
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
  }

  private void guard(int softCap) {
    f.limits.set(new com.pangate.gateway.config.GatewayLimits(700_000, softCap, 200, 65_536, 8_192,
        Duration.ofSeconds(5), Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100, 32, 65_536, Duration.ofSeconds(5), 10_000, Duration.ofSeconds(30)));
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
        Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100, 32, 65_536, Duration.ofSeconds(5), 10_000, Duration.ofSeconds(30)));

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

  @Test
  void shedLevel3Answers503WithRetryAfter() {
    node.shedLevel(3);
    guard(550_000);
    f.channel.writeInbound(upgrade("/v1/ws", "pangate.v1"));
    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(503);
    assertThat(Integer.parseInt(r.headers().get("Retry-After"))).isBetween(5, 30);
    r.release();
  }

  private void guardWithUpstream(int softCap) {
    f.limits.set(new com.pangate.gateway.config.GatewayLimits(700_000, softCap, 200, 65_536, 8_192,
        Duration.ofSeconds(5), Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100, 32, 65_536, Duration.ofSeconds(5), 10_000, Duration.ofSeconds(30)));
    f.channel.pipeline().addLast("upgradeGuard", new UpgradeGuard(props(softCap), f.limits, f.loops, node,
        new SplittableRandom(1), java.util.Map.of("/v1/upstream",
            () -> new io.netty.channel.ChannelInboundHandlerAdapter())));
    f.channel.pipeline().addLast("ws", new io.netty.channel.ChannelInboundHandlerAdapter());
    f.channel.pipeline().addLast("session", new io.netty.channel.ChannelInboundHandlerAdapter());
  }

  @Test
  void upstreamPathNeedsNoSubprotocolAndRewiresThePipeline() {
    guardWithUpstream(550_000);

    FullHttpRequest r = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/upstream");
    f.channel.writeInbound(r);

    assertThat(f.channel.pipeline().get("httpUpstream")).isNotNull();
    assertThat(f.channel.pipeline().get("ws")).isNull();
    assertThat(f.channel.pipeline().get("session")).isNull();
    assertThat(f.channel.pipeline().get("upgradeGuard")).isNull();
    Object passed = f.channel.readInbound();
    assertThat(passed).isSameAs(r);
    r.release();
  }

  @Test
  void upstreamPathStill503WhenShedding() {
    node.shedLevel(3);
    guardWithUpstream(550_000);

    f.channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/upstream"));

    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(503);
    r.release();
  }

  @Test
  void ssePathIs404UntilRouted() {
    guardWithUpstream(550_000);

    f.channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/sse"));

    FullHttpResponse r = response();
    assertThat(r.status().code()).isEqualTo(404);
    r.release();
  }

  @Test
  void routedPathAlsoDropsTheHandshakeHandlerNettyAddsForWebSocket() {
    f.limits.set(new com.pangate.gateway.config.GatewayLimits(700_000, 550_000, 200, 65_536, 8_192,
        Duration.ofSeconds(5), Duration.ofSeconds(10), 20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100, 32, 65_536, Duration.ofSeconds(5), 10_000, Duration.ofSeconds(30)));
    java.util.List<Object> seen = new java.util.ArrayList<>();
    f.channel.pipeline().addLast("upgradeGuard", new UpgradeGuard(props(550_000), f.limits, f.loops, node,
        new SplittableRandom(1), java.util.Map.of("/v1/sse", () -> new io.netty.channel.ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
            io.netty.util.ReferenceCountUtil.release(msg);
            ctx.fireChannelRead(io.netty.buffer.Unpooled.wrappedBuffer(new byte[] {1}));  // like SseAdapter
          }
        })));
    f.channel.pipeline().addLast("ws", new io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler(
        "/v1/ws", "pangate.v1"));
    f.channel.pipeline().addLast("tail", new io.netty.channel.ChannelInboundHandlerAdapter() {
      @Override
      public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
        seen.add(msg);
      }
    });

    f.channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/sse"));

    assertThat(f.channel.pipeline().names()).noneMatch(n -> n.contains("Handshake"));
    assertThat(seen).singleElement().isInstanceOf(io.netty.buffer.ByteBuf.class);
    ((io.netty.buffer.ByteBuf) seen.get(0)).release();
  }
}
