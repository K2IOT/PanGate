package com.pangate.gateway.pipeline;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.FrameCodec;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.flush.FlushConsolidationHandler;
import java.util.concurrent.ThreadLocalRandom;

/** WSS pipeline (spec §4.1). HTTP codecs are removed by Netty after the upgrade. */
public final class WsChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final PipelineDeps deps;
  private final com.pangate.gateway.net.CidrSet proxyTrusted;  // null = PROXY protocol off
  private final WebSocketServerProtocolConfig wsConfig;
  // Stateless handlers shared by every connection (fewer objects per idle session).
  private final WsBinaryAdapter wsBinary = new WsBinaryAdapter();
  private final FrameCodec frameCodec;
  private final SessionHandler sessionHandler;
  private final HttpUpstreamHandler httpUpstream;
  private final java.util.Map<String, java.util.function.Supplier<io.netty.channel.ChannelHandler>> routes;

  public WsChannelInitializer(PipelineDeps deps) {
    this.deps = deps;
    GatewayProperties.ProxyProtocol proxy = deps.props().listeners().proxyProtocol();
    this.proxyTrusted = proxy.enabled() ? com.pangate.gateway.net.CidrSet.parse(proxy.trustedCidrs()) : null;
    this.frameCodec = new FrameCodec(deps.limits());
    this.sessionHandler = deps.sessionHandler();
    this.httpUpstream = new HttpUpstreamHandler(deps.tickets(), deps.policy(), deps.upstream(), deps.limits(),
        deps.metrics(), () -> System.currentTimeMillis() / 1000);
    java.util.concurrent.atomic.AtomicInteger sseStreams = new java.util.concurrent.atomic.AtomicInteger();
    deps.metrics().gauge("gw.sse.sessions", sseStreams, java.util.concurrent.atomic.AtomicInteger::get);
    this.routes = java.util.Map.of("/v1/upstream", () -> httpUpstream,
        "/v1/sse", () -> new SseAdapter(java.time.Duration.ofSeconds(30), sseStreams, deps.limits()));
    this.wsConfig = WebSocketServerProtocolConfig.newBuilder()
        .websocketPath(deps.props().listeners().ws().path())
        .subprotocols(UpgradeGuard.SUBPROTOCOL)
        // UpgradeGuard already enforces the exact path (query ignored); let Netty accept "/v1/ws?query".
        .checkStartsWith(true)
        .handleCloseFrames(false)
        .handshakeTimeoutMillis(10_000)
        .decoderConfig(WebSocketDecoderConfig.newBuilder()
            .maxFramePayloadLength(deps.limits().current().frameMax())
            .allowExtensions(false)
            .build())
        .build();
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    ch.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.WS);
    ChannelPipeline p = ch.pipeline();
    if (proxyTrusted != null) {
      p.addLast("proxy", new ProxyProtocolHandler(proxyTrusted, deps.props().listeners().proxyProtocol().trustLoopback(),
          deps.limits().current().helloTimeout(), deps.metrics()));
    }
    p.addLast("acceptGate", deps.acceptGate());
    p.addLast("tls", deps.tls().ws().newHandler(ch.alloc()));
    p.addLast("flush", new FlushConsolidationHandler(256, true));
    p.addLast("writability", WritabilityTracker.INSTANCE);
    p.addLast("http", new HttpServerCodec());
    // Upgrades are tiny, but POST /v1/upstream carries a whole frame (spec §4.3): up to frame-max plus slack.
    p.addLast("aggregator", new HttpObjectAggregator(Math.max(8192, deps.limits().current().frameMax() + 1024)));
    p.addLast("upgradeGuard", new UpgradeGuard(deps.props(), deps.limits(), deps.loops(), deps.node(),
        ThreadLocalRandom.current(), routes));
    p.addLast("ws", new WebSocketServerProtocolHandler(wsConfig));
    p.addLast("wsBinary", wsBinary);
    p.addLast("frameCodec", frameCodec);
    p.addLast("hello", deps.helloAuth());
    p.addLast("session", sessionHandler);
    p.addLast("errors", PipelineDeps.ERROR_TERMINATOR);
  }
}
