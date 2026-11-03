package com.pangate.gateway.pipeline;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.Reauth;
import com.pangate.gateway.auth.ConnectClaims;
import com.pangate.gateway.auth.JwtAuthFlow;
import com.pangate.gateway.auth.JwtResult;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.auth.ResumeTicket;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.Frames;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.upstream.UpstreamPublisher;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/** Frames of an authenticated session (spec §5): PING, REAUTH, UPSTREAM, RPC_RES; SUBSCRIBE answers UNSUPPORTED until M2b. */
@io.netty.channel.ChannelHandler.Sharable
public final class SessionHandler extends ChannelInboundHandlerAdapter {

  private final ResumeTicketService tickets;
  private final JwtAuthFlow jwt;
  private final PrincipalHandshakeLimiter limiter;
  private final com.pangate.gateway.auth.HelloPolicy policy;
  private final LimitsHolder limits;
  private final String cell;
  private final UpstreamPublisher upstream;

  public SessionHandler(ResumeTicketService tickets, JwtAuthFlow jwt, PrincipalHandshakeLimiter limiter,
      com.pangate.gateway.auth.HelloPolicy policy, LimitsHolder limits, String cell, UpstreamPublisher upstream) {
    this.tickets = tickets;
    this.jwt = jwt;
    this.limiter = limiter;
    this.policy = policy;
    this.limits = limits;
    this.cell = cell;
    this.upstream = upstream;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof Frame frame)) {
      ctx.fireChannelRead(msg);
      return;
    }
    Session session = ctx.channel().attr(Session.KEY).get();
    session.touch(session.loop().nowSec());
    long now = session.loop().wallSec();
    switch (frame.getType()) {
      case FRAME_TYPE_PING -> ctx.writeAndFlush(Frames.pong(frame.getMsgId()));
      case FRAME_TYPE_HELLO -> throw new ProtocolException(CloseCode.BAD_FRAME, "duplicate HELLO");
      case FRAME_TYPE_REAUTH -> reauth(ctx, session, frame, now);
      case FRAME_TYPE_ACK -> { }
      case FRAME_TYPE_UPSTREAM -> upstream.publish(ctx, session, frame, null);
      case FRAME_TYPE_RPC_RES -> upstream.publish(ctx, session, frame, "cmd_result");
      default -> ctx.writeAndFlush(Frames.error(CloseCode.INTERNAL.code(), "UNSUPPORTED", false, false));
    }
  }

  private void reauth(ChannelHandlerContext ctx, Session session, Frame frame, long now) {
    Reauth req;
    try {
      req = Reauth.parseFrom(frame.getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new ProtocolException(CloseCode.BAD_FRAME, "malformed REAUTH");
    }
    // Same per-principal budget as HELLO (spec §5.6 → §4.6 h), checked before the ES256 verify it protects.
    if (!limiter.tryAcquire(session.identity().principalId())) {
      session.close(CloseCode.RATE_LIMITED, CloseCode.RATE_LIMITED.name());
      return;
    }
    jwt.verify(ctx.channel(), req.getJwt(), now, result -> {
      if (session.state() != SessionState.ACTIVE) {
        return;
      }
      if (!(result instanceof JwtResult.Verified v) || !sameOwner(session, v.claims())) {
        session.close(CloseCode.AUTH_FAILED, "AUTH_FAILED");
        return;
      }
      ConnectClaims c = v.claims();
      long exp = now + limits.current().sessionTtl().toSeconds();
      long jtiHash = ResumeTicketService.jtiHash(c.jti());
      var rejection = policy.checkPolicy(new com.pangate.gateway.auth.HelloPolicy.Candidate(cell, c.tenant(),
          c.principal(), c.device(), jtiHash, session.identity().sdkVersion(), session.origin()));
      if (rejection.isPresent()) {
        session.close(rejection.get().code(), rejection.get().code().name(), rejection.get().retryAfterMs());
        return;
      }
      String ticket = tickets.issue(new ResumeTicket(cell, c.tenant(), c.principal(), c.device(), now, exp, jtiHash,
          c.topics()));
      session.reauth(now, exp, jtiHash, c.topics());
      ctx.writeAndFlush(Frames.reauth(Reauth.newBuilder().setResumeTicket(ticket)
          .setSessionExpiresAtMs(exp * 1000).build()));
    });
  }

  private static boolean sameOwner(Session s, ConnectClaims c) {
    return s.identity().tenantId().equals(c.tenant()) && s.identity().principalId().equals(c.principal())
        && s.identity().deviceId().equals(c.device());
  }
}
