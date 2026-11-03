package com.pangate.gateway.pipeline;

import com.pangate.gateway.auth.JwtAuthFlow;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.SessionTracker;

/** Everything the channel initializers need to build per-connection pipelines. */
public record PipelineDeps(
    GatewayProperties props,
    LimitsHolder limits,
    LoopRegistry loops,
    NodeState node,
    AcceptGate acceptGate,
    SslContextProvider tls,
    ResumeTicketService tickets,
    JwtAuthFlow jwt,
    SessionTracker tracker,
    PrincipalHandshakeLimiter handshakeLimiter,
    GatewayMetrics metrics,
    SeqReader seq) {

  static final ErrorTerminator ERROR_TERMINATOR = new ErrorTerminator();

  HelloAuthHandler helloAuth() {
    return new HelloAuthHandler(tickets, jwt, tracker, handshakeLimiter, limits, props.cell(), metrics, seq);
  }

  SessionHandler sessionHandler() {
    return new SessionHandler(tickets, jwt, handshakeLimiter, limits, props.cell());
  }
}
