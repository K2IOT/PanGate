package com.pangate.gateway.pipeline;

import com.pangate.gateway.session.Session;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/** Feeds channel writability to the session's slow-consumer deadline (spec §7.4). Stateless, shared. */
@ChannelHandler.Sharable
public final class WritabilityTracker extends ChannelInboundHandlerAdapter {

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
}
