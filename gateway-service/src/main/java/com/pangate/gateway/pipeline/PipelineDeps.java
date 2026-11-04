package com.pangate.gateway.pipeline;

import com.pangate.gateway.auth.JwtAuthFlow;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.upstream.UpstreamPublisher;
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
    com.pangate.gateway.auth.HelloPolicy policy,
    com.pangate.gateway.control.TenantLimitsStore tenants,
    GatewayMetrics metrics,
    SeqReader seq,
    UpstreamPublisher upstream,
    com.pangate.gateway.downstream.PushDelivery delivery,
    com.pangate.gateway.downstream.ClientTopics clientTopics,
    com.pangate.gateway.tenant.TenantCap tenantCap) {

  static final ErrorTerminator ERROR_TERMINATOR = new ErrorTerminator();

  HelloAuthHandler helloAuth() {
    return new HelloAuthHandler(tickets, jwt, tracker, policy, tenants, limits, props.cell(), metrics, seq, tenantCap);
  }

  SessionHandler sessionHandler() {
    return new SessionHandler(tickets, jwt, handshakeLimiter, policy, limits, props.cell(), upstream, delivery, clientTopics);
  }
}
