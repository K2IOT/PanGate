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
    if (ch.attr(ChannelAttrs.TRANSPORT).get() == ChannelAttrs.TransportKind.SSE) {
      return closeSse(ch, code, reason, retryAfterMs);
    }
    if (ch.attr(ChannelAttrs.TRANSPORT).get() == ChannelAttrs.TransportKind.HTTP) {
      return httpStatus(ch, httpStatusFor(code), retryAfterMs);
    }
    if (ch.attr(ChannelAttrs.TRANSPORT).get() == ChannelAttrs.TransportKind.WS) {
      String text = retryAfterMs > 0 ? reason + " retryAfter=" + retryAfterMs : reason;
      return ch.writeAndFlush(new CloseWebSocketFrame(code.code(), text)).addListener(ChannelFutureListener.CLOSE);
    }
    return ch.writeAndFlush(Frames.error(code.code(), reason, code.retryable(), true, retryAfterMs))
        .addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * SSE (spec §4.3): before the stream started the client gets an HTTP status; once streaming, a fatal ERROR event and
   * the last chunk.
   */
  /** HTTP routes (/v1/upstream): the server ending a stateless request means "unavailable" unless auth-related. */
  private static io.netty.handler.codec.http.HttpResponseStatus httpStatusFor(CloseCode code) {
    io.netty.handler.codec.http.HttpResponseStatus s = com.pangate.gateway.pipeline.SseAdapter.httpStatus(code);
    return s.code() == 400 ? io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE : s;
  }

  private static ChannelFuture httpStatus(Channel ch, io.netty.handler.codec.http.HttpResponseStatus status,
      long retryAfterMs) {
    io.netty.handler.codec.http.FullHttpResponse res = new io.netty.handler.codec.http.DefaultFullHttpResponse(
        io.netty.handler.codec.http.HttpVersion.HTTP_1_1, status);
    res.headers().set(io.netty.handler.codec.http.HttpHeaderNames.CONNECTION,
        io.netty.handler.codec.http.HttpHeaderValues.CLOSE)
        .setInt(io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH, 0);
    if (retryAfterMs > 0) {
      res.headers().set(io.netty.handler.codec.http.HttpHeaderNames.RETRY_AFTER,
          Long.toString(Math.max(1, (retryAfterMs + 999) / 1000)));
    }
    return ch.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
  }

  private static ChannelFuture closeSse(Channel ch, CloseCode code, String reason, long retryAfterMs) {
    if (!ChannelAttrs.isSet(ch, ChannelAttrs.SSE_STREAMING)) {
      return httpStatus(ch, com.pangate.gateway.pipeline.SseAdapter.httpStatus(code), retryAfterMs);
    }
    ch.write(Frames.error(code.code(), reason, code.retryable(), true, retryAfterMs));
    return ch.writeAndFlush(io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT)
        .addListener(ChannelFutureListener.CLOSE);
  }
}
