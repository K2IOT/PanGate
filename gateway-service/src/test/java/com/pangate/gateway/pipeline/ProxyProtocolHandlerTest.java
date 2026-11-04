package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.net.CidrSet;
import com.pangate.gateway.protocol.ChannelAttrs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ProxyProtocolHandlerTest {

  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  private final List<String> events = new ArrayList<>();

  /** Stands in for AcceptGate: records when it sees channelActive and what bytes reach it. */
  private final class Recorder extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      events.add("active client=" + ChannelAttrs.clientAddress(ctx.channel()).getHostAddress());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuf b = (ByteBuf) msg;
      events.add("read " + b.toString(StandardCharsets.US_ASCII));
      b.release();
    }
  }

  private EmbeddedChannel channel(List<String> trusted) {
    EmbeddedChannel ch = new EmbeddedChannel(false, false);
    ch.pipeline().addLast("proxy", new ProxyProtocolHandler(CidrSet.parse(trusted), true, Duration.ofSeconds(5),
        new GatewayMetrics(meters)));
    ch.pipeline().addLast("acceptGate", new Recorder());
    register(ch);
    return ch;
  }

  private static void register(EmbeddedChannel ch) {
    try {
      ch.register();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static ByteBuf ascii(String s) {
    return Unpooled.copiedBuffer(s, StandardCharsets.US_ASCII);
  }

  @Test
  void channelActiveIsHeldUntilTheHeader() {
    channel(List.of("127.0.0.0/8"));
    assertThat(events).isEmpty();
  }

  @Test
  void trustedPeerHeaderSetsClientAddressBeforeAcceptGate() {
    EmbeddedChannel ch = channel(List.of("127.0.0.0/8"));
    ch.writeInbound(ascii("PROXY TCP4 203.0.113.7 10.0.0.1 5555 8443\r\nhello"));

    assertThat(events).containsExactly("active client=203.0.113.7", "read hello");
    assertThat(ch.pipeline().get("proxy")).isNull();
    assertThat(ch.pipeline().names()).noneMatch(n -> n.contains("Decoder") || n.equals("proxyDecoder"));
  }

  @Test
  void localCommandKeepsSocketAddress() {
    EmbeddedChannel ch = channel(List.of("127.0.0.0/8"));
    byte[] v2 = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x20, 0x00, 0x00, 0x00};
    ch.writeInbound(Unpooled.wrappedBuffer(v2));

    assertThat(events).containsExactly("active client=" + ChannelAttrs.socketPeer(ch).getHostAddress());
  }

  @Test
  void missingHeaderTimesOut() {
    EmbeddedChannel ch = channel(List.of("127.0.0.0/8"));
    ch.advanceTimeBy(5, TimeUnit.SECONDS);
    ch.runScheduledPendingTasks();
    assertThat(ch.isOpen()).isFalse();
    assertThat(events).isEmpty();
  }

  @Test
  void garbageInsteadOfHeaderIsRejected() {
    EmbeddedChannel ch = channel(List.of("127.0.0.0/8"));
    byte[] clientHello = new byte[200];  // a TLS ClientHello: longer than a v1 header (108 B), no CRLF
    clientHello[0] = 0x16;
    clientHello[1] = 0x03;
    ch.writeInbound(Unpooled.wrappedBuffer(clientHello));
    assertThat(ch.isOpen()).isFalse();
    assertThat(events).isEmpty();
    assertThat(meters.get("gw.admission.rejected").tag("reason", "proxy").counter().count()).isEqualTo(1);
  }

  @Test
  void untrustedPeerHeaderIsNotParsed() {
    EmbeddedChannel ch = channel(List.of("10.0.0.0/8"));
    assertThat(events).containsExactly("active client=" + ChannelAttrs.socketPeer(ch).getHostAddress());

    ch.writeInbound(ascii("PROXY TCP4 203.0.113.7 10.0.0.1 5555 8443\r\n"));
    assertThat(events).hasSize(2);
    assertThat(events.get(1)).startsWith("read PROXY TCP4 203.0.113.7");
    assertThat(ChannelAttrs.clientAddress(ch)).isEqualTo(ChannelAttrs.socketPeer(ch));
  }

  @Test
  void loopbackPeerIsDirectUnlessTrustLoopback() {
    EmbeddedChannel ch = new EmbeddedChannel(false, false);
    ch.pipeline().addLast("proxy", new ProxyProtocolHandler(CidrSet.parse(List.of("127.0.0.0/8")), false,
        Duration.ofSeconds(5), new GatewayMetrics(meters)));
    ch.pipeline().addLast(new Recorder());
    register(ch);
    assertThat(events).containsExactly("active client=" + InetAddress.getLoopbackAddress().getHostAddress());
  }
}
