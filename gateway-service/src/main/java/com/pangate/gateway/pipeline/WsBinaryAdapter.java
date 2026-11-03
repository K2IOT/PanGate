package com.pangate.gateway.pipeline;

import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.ProtocolException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;

/** WebSocket binary frames ↔ ByteBuf. Text and fragmented frames break the protocol; client close frames close. */
public final class WsBinaryAdapter extends ChannelDuplexHandler {

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof BinaryWebSocketFrame f && f.isFinalFragment()) {
      ctx.fireChannelRead(f.content());
      return;
    }
    if (msg instanceof CloseWebSocketFrame close) {
      ctx.channel().attr(ChannelAttrs.CLIENT_CLOSE_CODE).set(close.statusCode());
      close.release();
      ctx.close();
      return;
    }
    if (msg instanceof WebSocketFrame) {
      ReferenceCountUtil.release(msg);
      throw new ProtocolException(CloseCode.BAD_FRAME, "only unfragmented binary frames are allowed");
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    ctx.write(msg instanceof ByteBuf buf ? new BinaryWebSocketFrame(buf) : msg, promise);
  }
}
