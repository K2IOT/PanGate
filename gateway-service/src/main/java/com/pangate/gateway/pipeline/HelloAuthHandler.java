package com.pangate.gateway.pipeline;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.gateway.auth.ConnectClaims;
import com.pangate.gateway.auth.JwtAuthFlow;
import com.pangate.gateway.auth.JwtResult;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.auth.ResumeTicket;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.auth.TicketVerifyResult;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.Frames;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.session.SessionTracker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import java.util.ArrayDeque;
import java.util.HashSet;

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
  private final PrincipalHandshakeLimiter limiter;
  private final LimitsHolder limits;
  private final String cell;
  private final GatewayMetrics metrics;
  private final ArrayDeque<Object> held = new ArrayDeque<>();
  private boolean verifying;
  private boolean done;
  private boolean failed;

  public HelloAuthHandler(ResumeTicketService tickets, JwtAuthFlow jwt, SessionTracker tracker,
      PrincipalHandshakeLimiter limiter, LimitsHolder limits, String cell, GatewayMetrics metrics) {
    this.tickets = tickets;
    this.jwt = jwt;
    this.tracker = tracker;
    this.limiter = limiter;
    this.limits = limits;
    this.cell = cell;
    this.metrics = metrics;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
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
    if (!cell.equals(t.cell())) {
      fail(session, CloseCode.WRONG_CELL, "wrong_cell");
      return;
    }
    if (!limiter.tryAcquire(t.principal())) {
      fail(session, CloseCode.RATE_LIMITED, "rate_limited");
      return;
    }
    activate(ctx, session, hello, t);
  }

  private void withJwt(ChannelHandlerContext ctx, Session session, Hello hello, JwtResult result, long now) {
    if (!(result instanceof JwtResult.Verified v) || !v.claims().device().equals(hello.getDeviceId())) {
      fail(session, CloseCode.AUTH_FAILED, "auth_failed");
      return;
    }
    ConnectClaims c = v.claims();
    if (!limiter.tryAcquire(c.principal())) {
      fail(session, CloseCode.RATE_LIMITED, "rate_limited");
      return;
    }
    long exp = now + limits.current().sessionTtl().toSeconds();
    activate(ctx, session, hello, new ResumeTicket(cell, c.tenant(), c.principal(), c.device(), now, exp,
        ResumeTicketService.jtiHash(c.jti()), c.topics()));
  }

  private void activate(ChannelHandlerContext ctx, Session session, Hello hello, ResumeTicket t) {
    String ticket = tickets.issue(t);  // first: if this throws, no session state has been touched
    done = true;
    String connId = tracker.newConnId(session);
    session.activate(new Session.Identity(t.tenant(), t.principal(), t.device(), hello.getPlatform(),
        hello.getSdkVersion(), new HashSet<>(hello.getCapabilitiesList())), connId, t.authAtSec(), t.expSec(),
        t.authJtiHash(), t.topicPatterns());
    ctx.channel().attr(ChannelAttrs.AUTHENTICATED).set(true);
    tracker.onActivated(session);
    ctx.writeAndFlush(Frames.connected(Connected.newBuilder()
        .setConnId(connId)
        .setCell(cell)
        .setHeartbeatSec((int) limits.current().heartbeatInterval().toSeconds())
        .setResumeTicket(ticket)
        .setServerTimeMs(System.currentTimeMillis())
        .setSessionExpiresAtMs(t.expSec() * 1000)
        .setSeq(0)
        .setSeqEpoch(0)
        .setSeqUnknown(true)
        .build()));
    metrics.handshake("ok");
    while (!held.isEmpty()) {
      ctx.fireChannelRead(held.poll());
    }
    ctx.pipeline().remove(this);
  }

  private void fail(Session session, CloseCode code, String result) {
    failed = true;
    metrics.handshake(result);
    held.forEach(io.netty.util.ReferenceCountUtil::release);
    held.clear();
    session.close(code, code.name());
  }
}
