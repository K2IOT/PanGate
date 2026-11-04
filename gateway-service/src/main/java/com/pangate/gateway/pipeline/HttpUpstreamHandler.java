package com.pangate.gateway.pipeline;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.auth.HelloPolicy;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.auth.TicketVerifyResult;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.upstream.UpstreamPublisher;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.util.Optional;

/**
 * {@code POST /v1/upstream} (spec §4.3): {@code Authorization: Ticket <resume>}, body {@code Frame(UPSTREAM)} as
 * {@code application/x-protobuf}. The ticket is verified and checked like a HELLO's (cell, revocation, blocked), the
 * frame goes to Kafka with no session lookup. One request per connection (plan ruling R3): every response carries
 * {@code Connection: close} and the channel is closed after it. Stateless, shared.
 */
@Sharable
public final class HttpUpstreamHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

  static final String CONTENT_TYPE = "application/x-protobuf";
  /** Kafka may hold a record up to delivery.timeout.ms (10 s): answer 503 well before the session deadline. */
  static final java.time.Duration RESPONSE_TIMEOUT = java.time.Duration.ofSeconds(5);
  // Set on the first request: one request per connection (plan ruling R3); pipelined extras are dropped.
  private static final io.netty.util.AttributeKey<Boolean> SEEN = io.netty.util.AttributeKey.valueOf("pangate.httpSeen");
  private static final io.netty.util.AttributeKey<Boolean> ANSWERED =
      io.netty.util.AttributeKey.valueOf("pangate.httpAnswered");
  private static final String SCHEME = "Ticket ";

  private final ResumeTicketService tickets;
  private final HelloPolicy policy;
  private final UpstreamPublisher upstream;
  private final LimitsHolder limits;
  private final GatewayMetrics metrics;
  private final java.util.function.LongSupplier wallSec;
  private final com.pangate.gateway.control.TenantLimitsStore tenants;
  private final com.pangate.gateway.upstream.PrincipalRateLimiter rate;
  private static final io.netty.util.AttributeKey<String> CORS_ORIGIN =
      io.netty.util.AttributeKey.valueOf("pangate.corsOrigin");

  public HttpUpstreamHandler(ResumeTicketService tickets, HelloPolicy policy, UpstreamPublisher upstream,
      LimitsHolder limits, GatewayMetrics metrics, java.util.function.LongSupplier wallSec,
      com.pangate.gateway.control.TenantLimitsStore tenants, com.pangate.gateway.upstream.PrincipalRateLimiter rate) {
    this.tenants = tenants;
    this.rate = rate;
    this.wallSec = wallSec;
    this.tickets = tickets;
    this.policy = policy;
    this.upstream = upstream;
    this.limits = limits;
    this.metrics = metrics;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
    if (Boolean.TRUE.equals(ctx.channel().attr(SEEN).getAndSet(true))) {
      return;  // a pipelined second request: ignored (auto-released), the connection closes after the first
    }
    ctx.channel().config().setAutoRead(false);
    if (!HttpMethod.POST.equals(req.method())) {
      respond(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED, 0);
      return;
    }
    String type = req.headers().get(HttpHeaderNames.CONTENT_TYPE, "");
    if (!type.toLowerCase(java.util.Locale.ROOT).startsWith(CONTENT_TYPE)) {
      respond(ctx, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, 0);
      return;
    }
    if (req.content().readableBytes() > limits.current().frameMax()) {
      respond(ctx, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, 0);
      return;
    }
    String auth = req.headers().get(HttpHeaderNames.AUTHORIZATION, "");
    if (!auth.startsWith(SCHEME)) {
      respond(ctx, HttpResponseStatus.UNAUTHORIZED, 0);
      return;
    }
    TicketVerifyResult r = tickets.verify(auth.substring(SCHEME.length()).trim(), wallSec.getAsLong());
    if (!(r instanceof TicketVerifyResult.Valid valid)) {
      respond(ctx, HttpResponseStatus.UNAUTHORIZED, 0);
      return;
    }
    Optional<HelloPolicy.Rejection> rejection = policy.checkTicketUse(valid.ticket());
    if (rejection.isPresent()) {
      HelloPolicy.Rejection rej = rejection.get();
      if (rej.code() == CloseCode.RATE_LIMITED) {
        respond(ctx, HttpResponseStatus.TOO_MANY_REQUESTS, (int) (rej.retryAfterMs() / 1000));
      } else {
        respond(ctx, HttpResponseStatus.FORBIDDEN, 0);
      }
      return;
    }
    String tenant = valid.ticket().tenant();
    String origin = req.headers().get(HttpHeaderNames.ORIGIN);
    if (!policy.originAllowed(tenant, origin)) {
      respond(ctx, HttpResponseStatus.FORBIDDEN, 0);  // no CORS headers: the browser blocks the page from reading it
      return;
    }
    if (origin != null) {
      ctx.channel().attr(CORS_ORIGIN).set(origin);
    }
    var tp = tenants.policy(tenant);
    if (!rate.tryAcquire(valid.ticket().principal(), tp.inboundRate(), tp.inboundBurst())) {
      respond(ctx, HttpResponseStatus.TOO_MANY_REQUESTS, 1);
      return;
    }
    Frame f;
    try {
      f = Frame.parseFrom(new ByteBufInputStream(req.content()));
    } catch (InvalidProtocolBufferException e) {
      respond(ctx, HttpResponseStatus.BAD_REQUEST, 0);
      return;
    } catch (IOException e) {
      respond(ctx, HttpResponseStatus.BAD_REQUEST, 0);
      return;
    }
    if (f.getType() != FrameType.FRAME_TYPE_UPSTREAM) {
      respond(ctx, HttpResponseStatus.BAD_REQUEST, 0);
      return;
    }
    ctx.executor().schedule(() -> respond(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, 0),
        RESPONSE_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    upstream.publishHttp(valid.ticket(), f).whenComplete((outcome, e) -> ctx.executor().execute(() -> {
      if (e != null) {
        respond(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, 0);
        return;
      }
      switch (outcome) {
        case ACCEPTED -> respond(ctx, HttpResponseStatus.ACCEPTED, 0);
        case UNKNOWN_TYPE, MISSING_MSG_ID -> respond(ctx, HttpResponseStatus.BAD_REQUEST, 0);
        case UNAVAILABLE -> respond(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, 0);
      }
    }));
  }

  private void respond(ChannelHandlerContext ctx, HttpResponseStatus status, int retryAfterSec) {
    if (Boolean.TRUE.equals(ctx.channel().attr(ANSWERED).getAndSet(true))) {
      return;  // already answered (e.g. the 5 s timeout fired before a late Kafka ack)
    }
    metrics.counter("gw.http.upstream", "status", Integer.toString(status.code())).increment();
    if (!ctx.channel().isActive()) {
      return;
    }
    FullHttpResponse res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
    res.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE).setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
    if (retryAfterSec > 0) {
      res.headers().setInt(HttpHeaderNames.RETRY_AFTER, retryAfterSec);
    }
    String origin = ctx.channel().attr(CORS_ORIGIN).get();  // set only once the tenant allowed it
    if (origin != null) {
      Cors.allow(res.headers(), origin);
    }
    ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
  }
}
