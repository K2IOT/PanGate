package com.pangate.gateway.pipeline;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.gateway.auth.ConnectClaims;
import com.pangate.gateway.auth.JwtAuthFlow;
import com.pangate.gateway.auth.JwtResult;
import com.pangate.gateway.auth.HelloPolicy;
import com.pangate.gateway.auth.ResumeTicket;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.auth.TicketVerifyResult;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.Frames;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.session.SessionTracker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import java.util.ArrayDeque;
import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;

/**
 * Authenticates the first frame (spec §4.6): HELLO with a resume ticket (HMAC, offline) or a connect JWT (ES256,
 * offline; async JWKS refresh on an unknown kid). On success sends CONNECTED, hands any frames that arrived while
 * verifying to the next handler in order, and removes itself.
 */
public final class HelloAuthHandler extends ChannelInboundHandlerAdapter {

  static final String ALPN_PANGATE = "pangate.v1";

  private final ResumeTicketService tickets;
  private final JwtAuthFlow jwt;
  private final SessionTracker tracker;
  private final HelloPolicy policy;
  private final TenantLimitsStore tenants;
  private final LimitsHolder limits;
  private final String cell;
  private final GatewayMetrics metrics;
  private final SeqReader seq;
  private final com.pangate.gateway.tenant.TenantCap cap;
  private final ArrayDeque<Object> held = new ArrayDeque<>();
  private boolean verifying;
  private boolean done;
  private boolean failed;

  public HelloAuthHandler(ResumeTicketService tickets, JwtAuthFlow jwt, SessionTracker tracker,
      HelloPolicy policy, TenantLimitsStore tenants, LimitsHolder limits, String cell, GatewayMetrics metrics,
      SeqReader seq) {
    this(tickets, jwt, tracker, policy, tenants, limits, cell, metrics, seq,
        com.pangate.gateway.tenant.TenantCap.localOnly(metrics));
  }

  public HelloAuthHandler(ResumeTicketService tickets, JwtAuthFlow jwt, SessionTracker tracker,
      HelloPolicy policy, TenantLimitsStore tenants, LimitsHolder limits, String cell, GatewayMetrics metrics,
      SeqReader seq,
      com.pangate.gateway.tenant.TenantCap cap) {
    this.tickets = tickets;
    this.jwt = jwt;
    this.tracker = tracker;
    this.policy = policy;
    this.tenants = tenants;
    this.limits = limits;
    this.cell = cell;
    this.metrics = metrics;
    this.seq = seq;
    this.cap = cap;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
      dropWebSocketHandshakeState(ctx);
      protocolReady(ctx);
    } else if (evt instanceof SslHandshakeCompletionEvent tls
        && ctx.channel().attr(ChannelAttrs.TRANSPORT).get() == ChannelAttrs.TransportKind.TCP) {
      SslHandler ssl = ctx.pipeline().get(SslHandler.class);
      if (tls.isSuccess() && ssl != null && ALPN_PANGATE.equals(ssl.applicationProtocol())) {
        protocolReady(ctx);
      } else {
        ctx.close();
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  /**
   * After the upgrade Netty's protocol handler only answers pings and keeps the handshaker (URL, subprotocol array)
   * per connection; WsBinaryAdapter answers pings and handles close frames itself, so both are dropped to keep idle
   * sessions small.
   */
  @SuppressWarnings("deprecation")
  private static void dropWebSocketHandshakeState(ChannelHandlerContext ctx) {
    if (ctx.pipeline().get(WebSocketServerProtocolHandler.class) != null) {
      ctx.pipeline().remove(WebSocketServerProtocolHandler.class);
    }
    ctx.channel().attr(HANDSHAKER_KEY).getAndRemove();
  }

  private static final io.netty.util.AttributeKey<Object> HANDSHAKER_KEY = io.netty.util.AttributeKey.valueOf(
      io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker.class, "HANDSHAKER");

  private void protocolReady(ChannelHandlerContext ctx) {
    ctx.channel().attr(ChannelAttrs.PROTOCOL_READY).set(true);
    Session s = ctx.channel().attr(Session.KEY).get();
    s.markUpgraded(s.loop().nowSec());
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (failed) {
      io.netty.util.ReferenceCountUtil.release(msg);
      return;
    }
    if (verifying || done) {
      held.add(msg);
      return;
    }
    if (!(msg instanceof Frame frame) || frame.getType() != FrameType.FRAME_TYPE_HELLO) {
      // Anything that is not a Frame (e.g. an HTTP request that slipped past the upgrade) may hold pooled memory.
      io.netty.util.ReferenceCountUtil.release(msg);
      metrics.handshake("bad_frame");
      throw new ProtocolException(CloseCode.BAD_FRAME, "expected HELLO");
    }
    Hello hello;
    try {
      hello = Hello.parseFrom(frame.getPayload());
    } catch (InvalidProtocolBufferException e) {
      metrics.handshake("bad_frame");
      throw new ProtocolException(CloseCode.BAD_FRAME, "malformed HELLO");
    }
    Session session = ctx.channel().attr(Session.KEY).get();
    long now = session.loop().wallSec();
    switch (hello.getCredentialCase()) {
      case RESUME_TICKET -> withTicket(ctx, session, hello, now);
      case JWT -> {
        verifying = true;
        jwt.verify(ctx.channel(), hello.getJwt(), now, result -> {
          verifying = false;
          if (session.state() == SessionState.HANDSHAKING) {
            withJwt(ctx, session, hello, result, now);
          }
        });
      }
      default -> fail(session, CloseCode.AUTH_FAILED, "auth_failed");
    }
  }

  private void withTicket(ChannelHandlerContext ctx, Session session, Hello hello, long now) {
    TicketVerifyResult r = tickets.verify(hello.getResumeTicket(), now);
    if (r instanceof TicketVerifyResult.Expired) {
      fail(session, CloseCode.SESSION_EXPIRED, "expired");
      return;
    }
    if (!(r instanceof TicketVerifyResult.Valid valid) || !valid.ticket().device().equals(hello.getDeviceId())) {
      fail(session, CloseCode.AUTH_FAILED, "auth_failed");
      return;
    }
    ResumeTicket t = valid.ticket();
    if (com.pangate.gateway.auth.WarmUpTokens.isWarmUp(t.tenant())) {
      fail(session, CloseCode.AUTH_FAILED, "auth_failed");  // warm-up sessions are never resumable (spec §7.6)
      return;
    }
    proceed(ctx, session, hello, t.cell(), t);
  }

  private void withJwt(ChannelHandlerContext ctx, Session session, Hello hello, JwtResult result, long now) {
    if (!(result instanceof JwtResult.Verified v) || !v.claims().device().equals(hello.getDeviceId())) {
      fail(session, CloseCode.AUTH_FAILED, "auth_failed");
      return;
    }
    ConnectClaims c = v.claims();
    if (com.pangate.gateway.auth.WarmUpTokens.isWarmUp(c.tenant())
        && !ChannelAttrs.socketPeer(ctx.channel()).isLoopbackAddress()) {  // the socket, never a PROXY claim
      fail(session, CloseCode.AUTH_FAILED, "auth_failed");  // warm-up tokens only from this host (spec §7.6)
      return;
    }
    long exp = now + limits.current().sessionTtl().toSeconds();
    ResumeTicket t = new ResumeTicket(cell, c.tenant(), c.principal(), c.device(), now, exp,
        ResumeTicketService.jtiHash(c.jti()), c.topics());
    proceed(ctx, session, hello, cell, t);
  }

  /** Spec §4.6 step 3 c–h, then step i (tenant cap: local quota, else borrow through Redis), then activation. */
  private void proceed(ChannelHandlerContext ctx, Session session, Hello hello, String ticketCell, ResumeTicket t) {
    if (!admitted(session, hello, ticketCell, t)) {
      return;
    }
    if (ctx.channel().attr(ChannelAttrs.TRANSPORT).get() == ChannelAttrs.TransportKind.SSE
        && !tenants.policy(t.tenant()).sseFallbackEnabled()) {
      fail(session, CloseCode.AUTH_FAILED, "sse_disabled");  // kill switch sse.fallback.enabled (spec §7.8)
      return;
    }
    int max = tenants.policy(t.tenant()).maxConns();
    if (max <= 0 || com.pangate.gateway.auth.WarmUpTokens.isWarmUp(t.tenant())) {
      activate(ctx, session, hello, t);
      return;
    }
    if (cap.tryReserve(t.tenant(), max)) {
      session.tenantReserved(true);
      activateReserved(ctx, session, hello, t);
      return;
    }
    verifying = true;  // frames the client pipelines meanwhile are held, like during JWKS refresh
    ctx.channel().config().setAutoRead(false);
    cap.borrow(t.tenant(), max).thenAccept(ok -> session.loop().eventLoop().execute(() -> {
      verifying = false;
      if (session.state() != SessionState.HANDSHAKING || !ctx.channel().isActive()) {
        if (ok) {
          cap.release(t.tenant());  // closed while borrowing: give the granted slot back
        }
        releaseHeld();
        return;
      }
      if (!ok) {
        failed = true;
        metrics.handshake("tenant_cap");
        releaseHeld();
        session.close(CloseCode.OVERLOADED, "TENANT_CAP",
            5_000 + java.util.concurrent.ThreadLocalRandom.current().nextLong(25_001));
        return;
      }
      session.tenantReserved(true);
      activateReserved(ctx, session, hello, t);
    }));
  }

  /** The close listener only releases sessions that got an identity: release here if activation threw before. */
  private void activateReserved(ChannelHandlerContext ctx, Session session, Hello hello, ResumeTicket t) {
    try {
      activate(ctx, session, hello, t);
    } catch (RuntimeException e) {
      if (session.identity() == null && session.tenantReserved()) {
        session.tenantReserved(false);
        cap.release(t.tenant());
      }
      throw e;
    }
  }

  /** Spec §4.6 step 3 c–h; on rejection the session is closed with the check's code. */
  private boolean admitted(Session session, Hello hello, String ticketCell, ResumeTicket t) {
    var rejection = policy.check(new HelloPolicy.Candidate(ticketCell, t.tenant(), t.principal(), t.device(),
        t.authJtiHash(), hello.getSdkVersion(), session.origin()));
    rejection.ifPresent(r -> {
      failed = true;
      metrics.handshake(r.metricResult());
      releaseHeld();
      session.close(r.code(), r.code().name(), r.retryAfterMs());
    });
    return rejection.isEmpty();
  }

  private void activate(ChannelHandlerContext ctx, Session session, Hello hello, ResumeTicket t) {
    // First: if this throws, no session state has been touched. Warm-up sessions get no ticket, so nothing issued to
    // the loopback warm-up client can be replayed from elsewhere (spec §7.6).
    String ticket = com.pangate.gateway.auth.WarmUpTokens.isWarmUp(t.tenant()) ? "" : tickets.issue(t);
    done = true;
    String connId = tracker.newConnId(session);
    session.activate(new Session.Identity(t.tenant(), t.principal(), t.device(), hello.getPlatform(),
        hello.getSdkVersion(), java.util.Set.copyOf(hello.getCapabilitiesList())), connId, System.currentTimeMillis(),
        t.authAtSec(), t.expSec(),
        t.authJtiHash(), t.topicPatterns());
    ctx.channel().attr(ChannelAttrs.AUTHENTICATED).set(true);
    tracker.onActivated(session);  // listeners: registry write and lifecycle CONNECTED, in parallel with the seq read
    // A revoke handled between the policy check and indexing found no session to close: check once more now.
    if (policy.isRevoked(t.principal(), t.device(), t.authJtiHash())) {
      metrics.handshake("revoked");
      session.close(CloseCode.REVOKED, CloseCode.REVOKED.name());
      return;
    }
    // CONNECTED waits for seq (≤ 200 ms); frames the client pipelines meanwhile stay held, reads pause.
    ctx.channel().config().setAutoRead(false);
    CompletionStage<OptionalLong> read;
    try {
      read = session.warmUp() ? java.util.concurrent.CompletableFuture.completedFuture(OptionalLong.empty())
          : seq.read(t.principal());
    } catch (RuntimeException e) {
      read = java.util.concurrent.CompletableFuture.completedFuture(OptionalLong.empty());
    }
    read.whenComplete((v, e) -> session.loop().eventLoop().execute(() ->
        connected(ctx, session, t, connId, ticket, e == null && v != null ? v : OptionalLong.empty())));
  }

  private void connected(ChannelHandlerContext ctx, Session session, ResumeTicket t, String connId, String ticket,
      OptionalLong seqValue) {
    if (session.state() != SessionState.ACTIVE) {
      releaseHeld();
      return;
    }
    ctx.writeAndFlush(Frames.connected(Connected.newBuilder()
        .setConnId(connId)
        .setCell(cell)
        .setHeartbeatSec(tenants.policy(t.tenant()).heartbeatSec())
        .setResumeTicket(ticket)
        .setServerTimeMs(System.currentTimeMillis())
        .setSessionExpiresAtMs(t.expSec() * 1000)
        .setSeq(seqValue.orElse(0))
        .setSeqEpoch(seq.seqEpoch(t.principal()))
        .setSeqUnknown(seqValue.isEmpty())
        .build()));
    metrics.handshake(session.warmUp() ? "warmup" : "ok");
    while (!held.isEmpty()) {
      ctx.fireChannelRead(held.poll());
    }
    ctx.channel().config().setAutoRead(true);
    ctx.pipeline().remove(this);
  }

  private void releaseHeld() {
    held.forEach(io.netty.util.ReferenceCountUtil::release);
    held.clear();
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    releaseHeld();
  }

  private void fail(Session session, CloseCode code, String result) {
    failed = true;
    metrics.handshake(result);
    releaseHeld();
    session.close(code, code.name());
  }
}
