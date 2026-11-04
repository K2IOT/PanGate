package com.pangate.gateway.protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

/** Closes a client connection the way its transport understands (spec §4.2, §4.8); at most once per channel. */
public final class ChannelCloser {

  /** A close frame that cannot be flushed (client stopped reading) must not keep the channel and its counters. */
  public static final java.time.Duration FORCE_CLOSE_AFTER = java.time.Duration.ofSeconds(5);

  private ChannelCloser() {}

  public static ChannelFuture close(Channel ch, CloseCode code, String reason, long retryAfterMs) {
    if (Boolean.TRUE.equals(ch.attr(ChannelAttrs.CLOSING).getAndSet(true))) {
      return ch.closeFuture();
    }
    if (!ChannelAttrs.isSet(ch, ChannelAttrs.PROTOCOL_READY) || !ch.isActive()) {
      return ch.close();
    }
    // 4008 (slow consumer, L5 shed) on a channel that is not reading: a close frame would queue behind the backlog
    // and hold it for FORCE_CLOSE_AFTER while the budget already counts it as freed. Drop it now (spec §7.2, §7.3).
    if (code == CloseCode.SLOW_CONSUMER && !ch.isWritable()) {
      return ch.close();
    }
    ch.eventLoop().schedule(() -> {
      if (ch.isOpen()) {
        ch.close();
      }
    }, FORCE_CLOSE_AFTER.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    if (ch.attr(ChannelAttrs.TRANSPORT).get() == ChannelAttrs.TransportKind.WS) {
      String text = retryAfterMs > 0 ? reason + " retryAfter=" + retryAfterMs : reason;
      return ch.writeAndFlush(new CloseWebSocketFrame(code.code(), text)).addListener(ChannelFutureListener.CLOSE);
    }
    return ch.writeAndFlush(Frames.error(code.code(), reason, code.retryable(), true, retryAfterMs))
        .addListener(ChannelFutureListener.CLOSE);
  }
}
