package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.gateway.auth.WarmUpTokens;
import com.pangate.gateway.config.GatewayProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WarmUpRunnerTest {

  private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
  private final Set<String> devices = ConcurrentHashMap.newKeySet();
  private final WarmUpTokens tokens = new WarmUpTokens("pangate-gw", "");

  @AfterEach
  void stop() {
    group.shutdownGracefully();
  }

  /** Stand-in gateway: WSS with subprotocol pangate.v1, CONNECTED for HELLO, PONG for PING. */
  @SuppressWarnings("deprecation")
  private int fakeGateway() throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    SslContext ssl = SslContextBuilder.forServer(cert.certificate(), cert.privateKey()).build();
    Channel ch = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel c) {
            c.pipeline().addLast(ssl.newHandler(c.alloc()), new HttpServerCodec(), new HttpObjectAggregator(8192),
                new WebSocketServerProtocolHandler("/v1/ws", "pangate.v1"),
                new SimpleChannelInboundHandler<BinaryWebSocketFrame>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame msg) throws Exception {
                    Frame in = Frame.parseFrom(ByteBufUtil.getBytes(msg.content()));
                    Frame out = switch (in.getType()) {
                      case FRAME_TYPE_HELLO -> {
                        devices.add(Hello.parseFrom(in.getPayload()).getDeviceId());
                        yield Frame.newBuilder().setType(FrameType.FRAME_TYPE_CONNECTED).build();
                      }
                      case FRAME_TYPE_PING -> Frame.newBuilder().setType(FrameType.FRAME_TYPE_PONG).build();
                      default -> null;
                    };
                    if (out != null) {
                      ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(out.toByteArray())));
                    }
                  }
                });
          }
        }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
    return ((InetSocketAddress) ch.localAddress()).getPort();
  }

  /** The port is the listener's bound one, given at run time (ws.port=0 binds an ephemeral port). */
  private WarmUpRunner.Result run(int iterations, int port, Duration timeout) {
    return new WarmUpRunner(new GatewayProperties.Warmup(iterations, 4, timeout), "/v1/ws", tokens,
        () -> System.currentTimeMillis() / 1000).run(port);
  }

  @Test
  void runsAllIterationsAgainstTheGateway() throws Exception {
    WarmUpRunner.Result r = run(20, fakeGateway(), Duration.ofSeconds(30));
    assertThat(r.ok()).isEqualTo(20);
    assertThat(r.failed()).isZero();
    assertThat(r.timedOut()).isFalse();
    assertThat(devices).hasSize(20).allMatch(d -> d.startsWith("warmup-"));
  }

  @Test
  void zeroIterationsIsANoOp() {
    assertThat(run(0, 1, Duration.ofSeconds(1))).isEqualTo(new WarmUpRunner.Result(0, 0, false));
  }

  @Test
  void unreachableGatewayFailsEveryIterationWithinTheTimeout() {
    long t0 = System.nanoTime();
    WarmUpRunner.Result r = run(10, 1, Duration.ofSeconds(1));  // nothing listens on port 1
    assertThat(r.ok()).isZero();
    assertThat(r.failed() + (r.timedOut() ? 1 : 0)).isPositive();
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(3));
  }
}
