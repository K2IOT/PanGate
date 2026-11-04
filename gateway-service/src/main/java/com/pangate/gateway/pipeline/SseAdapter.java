package com.pangate.gateway.pipeline;

import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SSE fallback {@code POST /v1/sse} (spec §4.3). Inbound: the request body is a {@code Hello}, handed to the codec
 * as a {@code Frame(HELLO)}; nothing else is read. Outbound: every encoded frame becomes an event
 * {@code data: <base64>}, the first one preceded by the {@code 200 text/event-stream} head. A comment line every
 * {@code heartbeat} keeps proxies from timing out and, once written, counts as a sign of life: the client cannot
 * PING on this transport. One instance per channel.
 */
public final class SseAdapter extends ChannelDuplexHandler {

  private static final ByteBuf COMMENT =
      Unpooled.unreleasableBuffer(Unpooled.copiedBuffer(":\n\n", StandardCharsets.US_ASCII));

  private final Duration heartbeat;
  private final AtomicInteger streams;
  private final com.pangate.gateway.config.LimitsHolder limits;
  private boolean helloSeen;
  private boolean streaming;
  private ScheduledFuture<?> beat;

  public SseAdapter(Duration heartbeat, AtomicInteger streams, com.pangate.gateway.config.LimitsHolder limits) {
    this.limits = limits;
    this.heartbeat = heartbeat;
    this.streams = streams;
  }

  /**
   * A client gone without FIN: comments land in the socket buffer and "succeed", so only TCP notices. On epoll,
   * TCP_USER_TIMEOUT bounds how long unacknowledged data may sit (Linux), instead of ~15 min of retransmissions.
   */
  static void applyUserTimeout(io.netty.channel.Channel ch, Duration timeout) {
    if (ch instanceof io.netty.channel.epoll.EpollSocketChannel) {
      ch.config().setOption(io.netty.channel.epoll.EpollChannelOption.TCP_USER_TIMEOUT, (int) timeout.toMillis());
    }
  }

  /** The status for a close before the stream started. */
  public static HttpResponseStatus httpStatus(CloseCode c) {
    return switch (c.code()) {
      case 4001, 4002 -> HttpResponseStatus.UNAUTHORIZED;
      case 4003 -> HttpResponseStatus.FORBIDDEN;
      case 4009 -> HttpResponseStatus.TOO_MANY_REQUESTS;
      case 4011 -> HttpResponseStatus.MISDIRECTED_REQUEST;
      case 4013, 1001, 4004, 4500 -> HttpResponseStatus.SERVICE_UNAVAILABLE;  // drain, timeouts: retry later
      default -> HttpResponseStatus.BAD_REQUEST;
    };
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (helloSeen || !(msg instanceof FullHttpRequest req)) {
      ReferenceCountUtil.release(msg);  // the client never sends more on this transport
      return;
    }
    helloSeen = true;
    try {
      HttpResponseStatus bad = validate(ctx, req);
      if (bad != null) {
        FullHttpResponse res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, bad);
        res.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE).setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
        return;
      }
      ctx.channel().attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.SSE);
      ctx.channel().attr(ChannelAttrs.PROTOCOL_READY).set(true);
      Session s = ctx.channel().attr(Session.KEY).get();
      if (s != null) {
        s.origin(req.headers().get(HttpHeaderNames.ORIGIN));
        s.markUpgraded(s.loop().nowSec());
      }
      Frame hello = Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO)
          .setPayload(com.google.protobuf.ByteString.copyFrom(ByteBufUtil.getBytes(req.content()))).build();
      ctx.fireChannelRead(Unpooled.wrappedBuffer(hello.toByteArray()));
    } finally {
      req.release();
    }
  }

  private HttpResponseStatus validate(ChannelHandlerContext ctx, FullHttpRequest req) {
    if (!HttpMethod.POST.equals(req.method())) {
      return HttpResponseStatus.METHOD_NOT_ALLOWED;
    }
    String type = req.headers().get(HttpHeaderNames.CONTENT_TYPE, "");
    if (!type.toLowerCase(java.util.Locale.ROOT).startsWith(HttpUpstreamHandler.CONTENT_TYPE)) {
      return HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
    }
    if (req.content().readableBytes() > limits.current().helloMax()) {
      return HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
    }
    return null;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (!(msg instanceof ByteBuf frame)) {
      ctx.write(msg, promise);  // HTTP objects from ChannelCloser
      return;
    }
    if (!streaming) {
      start(ctx);
    }
    String event;
    try {
      event = "data: " + Base64.getEncoder().encodeToString(ByteBufUtil.getBytes(frame)) + "\n\n";
    } finally {
      frame.release();
    }
    ctx.write(new DefaultHttpContent(Unpooled.copiedBuffer(event, StandardCharsets.US_ASCII)), promise);
  }

  private void start(ChannelHandlerContext ctx) {
    streaming = true;
    ctx.channel().attr(ChannelAttrs.SSE_STREAMING).set(true);
    streams.incrementAndGet();
    HttpResponse head = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    head.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/event-stream")
        .set(HttpHeaderNames.CACHE_CONTROL, HttpHeaderValues.NO_CACHE)
        .set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    Session owner = ctx.channel().attr(Session.KEY).get();
    if (owner != null && owner.origin() != null) {
      Cors.allow(head.headers(), owner.origin());  // HELLO already checked it against the tenant allowlist (§4.6 g)
    }
    ctx.write(head);
    // The comment is this transport's only sign of life (idle timeout = 3 × heartbeat): never wait longer than the
    // tenant's heartbeat interval.
    Session s = ctx.channel().attr(Session.KEY).get();
    long ms = s == null ? heartbeat.toMillis()
        : Math.max(1_000, Math.min(heartbeat.toMillis(), s.heartbeatSec() * 1000));
    beat = ctx.executor().scheduleAtFixedRate(() -> keepAlive(ctx), ms, ms, TimeUnit.MILLISECONDS);
    applyUserTimeout(ctx.channel(), Duration.ofMillis(3 * ms));
  }

  private void keepAlive(ChannelHandlerContext ctx) {
    if (!ctx.channel().isActive()) {
      return;
    }
    ctx.writeAndFlush(new DefaultHttpContent(COMMENT.duplicate())).addListener(f -> {
      Session s = ctx.channel().attr(Session.KEY).get();
      if (f.isSuccess() && s != null) {
        s.touch(s.loop().nowSec());
      }
    });
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    stop();
    ctx.fireChannelInactive();
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    stop();
  }

  private void stop() {
    if (beat != null) {
      beat.cancel(false);
      beat = null;
    }
    if (streaming) {
      streaming = false;
      streams.decrementAndGet();
    }
  }
}
