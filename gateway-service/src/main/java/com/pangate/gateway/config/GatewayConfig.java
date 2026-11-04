package com.pangate.gateway.config;

import com.pangate.gateway.auth.JwksCache;
import com.pangate.gateway.auth.JwksSources;
import com.pangate.gateway.auth.JwtAuthFlow;
import com.pangate.gateway.auth.JwtVerifier;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.auth.TicketKeyRing;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.EventLoopLagProbe;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.mgmt.AdminController;
import com.pangate.gateway.mgmt.EventLoopLagHealthIndicator;
import com.pangate.gateway.mgmt.GatewayReadinessHealthIndicator;
import com.pangate.gateway.mgmt.ReadinessCheck;
import com.pangate.gateway.pipeline.AcceptGate;
import com.pangate.gateway.pipeline.PipelineDeps;
import com.pangate.gateway.pipeline.SslContextProvider;
import com.pangate.gateway.pipeline.TcpChannelInitializer;
import com.pangate.gateway.pipeline.WsChannelInitializer;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.upstream.UpstreamPublisher;
import com.pangate.gateway.server.IoGroups;
import com.pangate.gateway.server.NettyGatewayServer;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.server.TickScheduler;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.session.SessionListener;
import com.pangate.gateway.session.SessionTracker;
import com.pangate.gateway.shed.AdmissionController;
import com.pangate.gateway.shed.IpConnectionCounter;
import com.pangate.starter.secrets.SecretSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wires the data plane. Order of start/stop is driven by SmartLifecycle phases (spec §3.7). */
@Configuration(proxyBeanMethods = false)
public class GatewayConfig {

  private static final int WHEEL_SLOTS = 128;

  @Bean
  LimitsHolder limitsHolder(GatewayProperties p) {
    return new LimitsHolder(GatewayLimits.from(p));
  }

  @Bean
  NodeState nodeState() {
    return new NodeState();
  }

  @Bean(destroyMethod = "shutdown")
  TickScheduler tickScheduler() {
    return new TickScheduler();
  }

  @Bean(destroyMethod = "shutdownNow")
  ExecutorService offloadExecutor() {
    return Executors.newVirtualThreadPerTaskExecutor();
  }

  @Bean
  IoGroups ioGroups(GatewayProperties p) {
    return new IoGroups(p.io());
  }

  @Bean
  LoopRegistry loopRegistry(IoGroups io) {
    return LoopRegistry.create(io.group(), WHEEL_SLOTS, () -> System.nanoTime() / 1_000_000_000L,
        () -> System.currentTimeMillis() / 1000);
  }

  @Bean
  GatewayMetrics gatewayMetrics(MeterRegistry registry, LoopRegistry loops) {
    GatewayMetrics m = new GatewayMetrics(registry);
    m.bindLoops(loops);
    return m;
  }

  @Bean
  AdmissionController admissionController(GatewayProperties p, LoopRegistry loops, NodeState node) {
    return new AdmissionController(p.admission(), loops.size(), System::nanoTime, node::shedLevel);
  }

  @Bean
  IpConnectionCounter ipConnectionCounter() {
    return new IpConnectionCounter();
  }

  @Bean
  PrincipalIndex principalIndex() {
    return new PrincipalIndex();
  }

  @Bean
  SessionTracker sessionTracker(LoopRegistry loops, PrincipalIndex principals, IpConnectionCounter ips,
      LimitsHolder limits, GatewayMetrics metrics, List<SessionListener> listeners,
      com.pangate.gateway.control.TenantLimitsStore tenants) {
    return new SessionTracker(loops, principals, ips, limits, metrics, System::currentTimeMillis, listeners, tenants);
  }

  @Bean
  ResumeTicketService resumeTicketService(SecretSource secrets) {
    return new ResumeTicketService(TicketKeyRing.load(secrets));
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
  JwksCache jwksCache(GatewayProperties p, TickScheduler tick, ExecutorService offload, GatewayMetrics metrics) {
    HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).executor(offload).build();
    JwksCache cache = new JwksCache(JwksSources.from(p.auth(), http), tick.executor(), offload,
        p.auth().jwksRefresh(), System::currentTimeMillis);
    metrics.bindJwks(cache);
    return cache;
  }

  @Bean
  com.pangate.gateway.auth.WarmUpTokens warmUpTokens(GatewayProperties p) {
    return new com.pangate.gateway.auth.WarmUpTokens(p.auth().audience(), p.auth().issuer());
  }

  @Bean
  JwtAuthFlow jwtAuthFlow(GatewayProperties p, JwksCache jwks, com.pangate.gateway.auth.WarmUpTokens warmUp) {
    JwtVerifier verifier = new JwtVerifier(jwks, p.auth().audience(), p.auth().issuer(), p.auth().clockSkew(),
        warmUp);
    return new JwtAuthFlow(verifier, jwks, Duration.ofSeconds(2));
  }

  @Bean
  PrincipalHandshakeLimiter principalHandshakeLimiter(LimitsHolder limits, TickScheduler tick) {
    PrincipalHandshakeLimiter limiter =
        new PrincipalHandshakeLimiter(limits.current().handshakesPerPrincipalPerMin(), System::currentTimeMillis);
    tick.scheduleAtFixedRate(limiter::sweepIdle, Duration.ofSeconds(30));
    return limiter;
  }

  @Bean
  SslContextProvider sslContextProvider(GatewayProperties p) {
    return new SslContextProvider(p.tls());
  }

  @Bean
  PipelineDeps pipelineDeps(GatewayProperties p, LimitsHolder limits, LoopRegistry loops, NodeState node,
      IpConnectionCounter ips, AdmissionController admission, SessionTracker tracker, GatewayMetrics metrics,
      SslContextProvider tls, ResumeTicketService tickets, JwtAuthFlow jwt, PrincipalHandshakeLimiter limiter,
      SeqReader seq, UpstreamPublisher upstream, com.pangate.gateway.auth.HelloPolicy policy,
      com.pangate.gateway.control.TenantLimitsStore tenants,
      com.pangate.gateway.downstream.PushDelivery delivery, com.pangate.gateway.downstream.ClientTopics clientTopics) {
    AcceptGate gate = new AcceptGate(node, loops, limits, ips, admission, tracker, metrics);
    return new PipelineDeps(p, limits, loops, node, gate, tls, tickets, jwt, tracker, limiter, policy, tenants,
        metrics, seq, upstream, delivery, clientTopics);
  }

  @Bean
  EventLoopLagProbe eventLoopLagProbe(LoopRegistry loops, TickScheduler tick, MeterRegistry registry) {
    EventLoopLagProbe probe = new EventLoopLagProbe(loops, tick, System::nanoTime);
    probe.bindTo(registry);
    return probe;
  }

  @Bean
  NettyGatewayServer nettyGatewayServer(GatewayProperties p, IoGroups io, LoopRegistry loops, NodeState node,
      AdmissionController admission, EventLoopLagProbe lag, PipelineDeps deps,
      com.pangate.gateway.lifecycle.DrainCoordinator drain) {
    return new NettyGatewayServer(p, io, loops, node, admission, lag, new WsChannelInitializer(deps),
        new TcpChannelInitializer(deps), drain);
  }

  @Bean
  ReadinessCheck listenersBoundCheck(NodeState node) {
    return () -> node.listenersBound() ? Optional.empty() : Optional.of("listeners not bound");
  }

  @Bean
  ReadinessCheck notDrainingCheck(NodeState node) {
    return () -> node.draining() ? Optional.of("draining") : Optional.empty();
  }

  @Bean
  ReadinessCheck belowSoftCapCheck(LoopRegistry loops, LimitsHolder limits) {
    return () -> loops.totalActive() >= limits.current().softCap()
        ? Optional.of("connections at soft cap") : Optional.empty();
  }

  @Bean("gatewayReadiness")
  GatewayReadinessHealthIndicator gatewayReadiness(List<ReadinessCheck> checks) {
    return new GatewayReadinessHealthIndicator(checks);
  }

  @Bean("eventLoopLag")
  EventLoopLagHealthIndicator eventLoopLag(EventLoopLagProbe probe) {
    return new EventLoopLagHealthIndicator(probe::maxP99Seconds);
  }

  @Bean
  AdminController adminController(NettyGatewayServer server) {
    // preStop: drain only; the SIGTERM that follows waits for the same drain in stop() (spec §7.5).
    return new AdminController(server::drain);
  }
}
