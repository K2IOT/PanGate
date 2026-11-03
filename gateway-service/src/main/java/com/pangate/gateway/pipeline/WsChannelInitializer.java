package com.pangate.gateway.pipeline;

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
  private final WebSocketServerProtocolConfig wsConfig;

  public WsChannelInitializer(PipelineDeps deps) {
    this.deps = deps;
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
    p.addLast("acceptGate", deps.acceptGate());
    p.addLast("tls", deps.tls().ws().newHandler(ch.alloc()));
    p.addLast("flush", new FlushConsolidationHandler(256, true));
    p.addLast("http", new HttpServerCodec());
    p.addLast("aggregator", new HttpObjectAggregator(8192));
    p.addLast("upgradeGuard", new UpgradeGuard(deps.props(), deps.loops(), deps.node(), ThreadLocalRandom.current()));
    p.addLast("ws", new WebSocketServerProtocolHandler(wsConfig));
    p.addLast("wsBinary", new WsBinaryAdapter());
    p.addLast("frameCodec", new FrameCodec(deps.limits()));
    p.addLast("hello", deps.helloAuth());
    p.addLast("session", deps.sessionHandler());
    p.addLast("errors", PipelineDeps.ERROR_TERMINATOR);
  }
}
