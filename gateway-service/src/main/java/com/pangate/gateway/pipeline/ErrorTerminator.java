package com.pangate.gateway.pipeline;

import com.pangate.gateway.protocol.ChannelCloser;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.ProtocolException;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Last handler: turns every exception into the right close; nothing escapes to the event loop. */
@Sharable
public final class ErrorTerminator extends ChannelInboundHandlerAdapter {

  private static final Logger log = LoggerFactory.getLogger(ErrorTerminator.class);
  private static final AtomicLong UNEXPECTED = new AtomicLong();

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ProtocolException pe = find(cause);
    if (pe != null) {
      ChannelCloser.close(ctx.channel(), pe.code(), pe.getMessage(), pe.code().defaultRetryAfterMs());
    } else if (cause instanceof IOException) {
      ctx.close();
    } else {
      if (UNEXPECTED.getAndIncrement() % 1000 == 0) {
        log.warn("unexpected error on {} (logging 1 in 1000)", ctx.channel(), cause);
      }
      ChannelCloser.close(ctx.channel(), CloseCode.INTERNAL, "INTERNAL", 0);
    }
  }

  private static ProtocolException find(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof ProtocolException pe) {
        return pe;
      }
    }
    return null;
  }
}
