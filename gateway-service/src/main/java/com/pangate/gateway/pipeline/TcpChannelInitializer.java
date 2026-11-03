package com.pangate.gateway.pipeline;

import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.FrameCodec;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.flush.FlushConsolidationHandler;

/** TCP+TLS pipeline (spec §4.2): 4-byte length prefix, same frames as WebSocket, ALPN pangate.v1. */
public final class TcpChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final PipelineDeps deps;
  private final FrameCodec frameCodec;
  private final SessionHandler sessionHandler;

  public TcpChannelInitializer(PipelineDeps deps) {
    this.deps = deps;
    this.frameCodec = new FrameCodec(deps.limits());
    this.sessionHandler = deps.sessionHandler();
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    ch.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    ChannelPipeline p = ch.pipeline();
    p.addLast("acceptGate", deps.acceptGate());
    p.addLast("tls", deps.tls().tcp().newHandler(ch.alloc()));
    p.addLast("flush", new FlushConsolidationHandler(256, true));
    p.addLast("lengthDecoder", new LengthFieldBasedFrameDecoder(deps.limits().current().frameMax() + 1024, 0, 4, 0, 4));
    p.addLast("lengthPrepender", new LengthFieldPrepender(4));
    p.addLast("frameCodec", frameCodec);
    p.addLast("hello", deps.helloAuth());
    p.addLast("session", sessionHandler);
    p.addLast("errors", PipelineDeps.ERROR_TERMINATOR);
  }
}
