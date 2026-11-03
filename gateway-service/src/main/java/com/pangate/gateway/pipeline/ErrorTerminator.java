package com.pangate.gateway.pipeline;

import com.pangate.gateway.protocol.ChannelCloser;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.session.Session;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Last handler: turns every exception into the right close; nothing escapes to the event loop. Protocol violations
 * (including oversized or corrupt frames) close with 4000, TLS garbage and resets close silently, anything else
 * closes with 4500. The close goes through the session when there is one, so close codes are recorded.
 */
@Sharable
public final class ErrorTerminator extends ChannelInboundHandlerAdapter {

  private static final Logger log = LoggerFactory.getLogger(ErrorTerminator.class);
  private static final AtomicLong UNEXPECTED = new AtomicLong();

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ProtocolException pe = find(cause, ProtocolException.class);
    if (pe != null) {
      close(ctx, pe.code(), pe.getMessage());
    } else if (find(cause, TooLongFrameException.class) != null
        || find(cause, CorruptedWebSocketFrameException.class) != null) {
      close(ctx, CloseCode.BAD_FRAME, "bad frame");
    } else if (find(cause, SSLException.class) != null || cause instanceof IOException) {
      ctx.close();
    } else {
      if (UNEXPECTED.getAndIncrement() % 1000 == 0) {
        log.warn("unexpected error on {} (logging 1 in 1000)", ctx.channel(), cause);
      }
      close(ctx, CloseCode.INTERNAL, "INTERNAL");
    }
  }

  private static void close(ChannelHandlerContext ctx, CloseCode code, String reason) {
    Session s = ctx.channel().attr(Session.KEY).get();
    if (s != null) {
      s.close(code, reason);
    } else {
      ChannelCloser.close(ctx.channel(), code, reason, code.defaultRetryAfterMs());
    }
  }

  private static <T extends Throwable> T find(Throwable t, Class<T> type) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (type.isInstance(c)) {
        return type.cast(c);
      }
    }
    return null;
  }
}
