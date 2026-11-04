package com.pangate.gateway.pipeline;

import com.pangate.gateway.session.Session;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelPromise;

/**
 * Feeds channel writability to the session's slow-consumer deadline (spec §7.4), and marks the session dirty on
 * every write so the outbound budget (spec §7.2) sees all frames — PONG, ACK, ERROR, REAUTH, RECONNECT, CONNECTED —
 * not only pushes. Sits right after TLS/flush so every protocol handler's writes pass it. Stateless, shared.
 */
@ChannelHandler.Sharable
public final class WritabilityTracker extends ChannelDuplexHandler {

  public static final WritabilityTracker INSTANCE = new WritabilityTracker();

  private WritabilityTracker() {}

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    Session s = ctx.channel().attr(Session.KEY).get();
    if (s != null) {
      s.writabilityChanged(ctx.channel().isWritable(), s.loop().nowSec());
    }
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    Session s = ctx.channel().attr(Session.KEY).get();
    if (s != null) {
      s.loop().markDirty(s);
    }
    ctx.write(msg, promise);
  }
}
