package com.pangate.gateway.config;

import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.auth.TicketKeyRing;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.pipeline.SslContextProvider;
import com.pangate.gateway.pipeline.TlsTicketKeys;
import com.pangate.gateway.server.TickScheduler;
import com.pangate.starter.secrets.SecretSource;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** M3b edge wiring: certificate and key reloads (file I/O on the offload executor; gw-tick only schedules). */
@Configuration(proxyBeanMethods = false)
public class EdgeConfig {

  private static final Logger log = LoggerFactory.getLogger(EdgeConfig.class);
  static final Duration KEY_RELOAD = Duration.ofSeconds(60);

  /** Spec §4.6 step i: live nodes of the cell, for the local quota. */
  @Bean
  com.pangate.gateway.tenant.CellSize cellSize(com.pangate.gateway.lifecycle.RedisConnections redis) {
    return new com.pangate.gateway.tenant.CellSize(redis, System::currentTimeMillis);
  }

  @Bean
  com.pangate.gateway.tenant.TenantCapBorrower tenantCapBorrower(GatewayProperties p,
      com.pangate.gateway.lifecycle.RedisConnections redis) {
    return new com.pangate.gateway.tenant.TenantCapBorrower(redis, p.nodeId(), System::currentTimeMillis);
  }

  /** Also a SessionListener: gives the slot back when a counted session closes. */
  @Bean
  com.pangate.gateway.tenant.TenantCap tenantCap(com.pangate.gateway.tenant.CellSize cells,
      com.pangate.gateway.tenant.TenantCapBorrower borrower, GatewayMetrics metrics,
      com.pangate.gateway.loop.LoopRegistry loops, com.pangate.gateway.control.TenantLimitsStore tenants) {
    return new com.pangate.gateway.tenant.TenantCap(new com.pangate.gateway.tenant.TenantConnCounter(), cells,
        borrower, metrics, loops, tenants);
  }

  /** Every 10 s on offload: refresh the cell size and report this node's tenant totals (30 s expiry, ruling R2). */
  @Bean
  ScheduledFuture<?> tenantCapReport(com.pangate.gateway.tenant.CellSize cells,
      com.pangate.gateway.tenant.TenantCapBorrower borrower, com.pangate.gateway.tenant.TenantCap cap,
      com.pangate.gateway.control.TenantLimitsStore tenants, TickScheduler tick, ExecutorService offload) {
    return tick.scheduleAtFixedRate(() -> offload.execute(() -> {
      cells.refresh();
      borrower.report(cap.counter().snapshot(), t -> tenants.policy(t).maxConns());
    }), Duration.ofSeconds(10));
  }

  /**
   * Push spans (spec §8). {@code gateway.tracing.otlp-endpoint} empty (default) = no-op; otherwise OTLP/HTTP with the
   * JDK HttpClient sender and a batch processor (never blocks the event loop).
   */
  @Bean(destroyMethod = "close")
  io.opentelemetry.sdk.OpenTelemetrySdk openTelemetry(GatewayProperties p,
      @org.springframework.beans.factory.annotation.Value("${gateway.tracing.otlp-endpoint:}") String endpoint) {
    var builder = io.opentelemetry.sdk.OpenTelemetrySdk.builder();
    if (!endpoint.isBlank()) {
      var resource = io.opentelemetry.sdk.resources.Resource.getDefault().merge(io.opentelemetry.sdk.resources.Resource
          .create(io.opentelemetry.api.common.Attributes.of(
              io.opentelemetry.api.common.AttributeKey.stringKey("service.name"), "gateway",
              io.opentelemetry.api.common.AttributeKey.stringKey("service.instance.id"), p.nodeId())));
      builder.setTracerProvider(io.opentelemetry.sdk.trace.SdkTracerProvider.builder().setResource(resource)
          .addSpanProcessor(new com.pangate.gateway.trace.LoopSafeSpanProcessor(
              io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter.builder().setEndpoint(endpoint).build(),
              16_384, Duration.ofSeconds(1)))
          .build());
      log.info("OTel spans exported to {}", endpoint);
    }
    io.opentelemetry.context.Context.current();  // set up context storage here, not on the first span on gw-io
    return builder.build();
  }

  @Bean
  com.pangate.gateway.trace.GatewayTracing gatewayTracing(io.opentelemetry.sdk.OpenTelemetrySdk otel,
      @org.springframework.beans.factory.annotation.Value("${gateway.tracing.otlp-endpoint:}") String endpoint) {
    com.pangate.gateway.trace.GatewayTracing t = endpoint.isBlank() ? com.pangate.gateway.trace.GatewayTracing.noop()
        : com.pangate.gateway.trace.GatewayTracing.create(otel);
    com.pangate.gateway.trace.GatewayTracing.install(t);  // for NACKs raised in Session (timeout, close)
    return t;
  }

  /** Kill switch trace.principal (spec §7.8): a control handler and a session listener. */
  @Bean
  com.pangate.gateway.trace.TraceTargets traceTargets(SecretSource secrets,
      com.pangate.gateway.session.PrincipalIndex principals, com.pangate.gateway.loop.LoopRegistry loops,
      TickScheduler tick) {
    com.pangate.gateway.trace.TraceTargets t = new com.pangate.gateway.trace.TraceTargets(principals, loops,
        com.pangate.gateway.trace.PrincipalHasher.load(secrets), System::currentTimeMillis);
    tick.scheduleAtFixedRate(t::sweepExpired, Duration.ofSeconds(10));
    return t;
  }

  /** Per-principal rate for POST /v1/upstream; idle buckets evicted every 60 s. */
  @Bean
  com.pangate.gateway.upstream.PrincipalRateLimiter upstreamRate(TickScheduler tick) {
    com.pangate.gateway.upstream.PrincipalRateLimiter r =
        new com.pangate.gateway.upstream.PrincipalRateLimiter(System::nanoTime);
    tick.scheduleAtFixedRate(r::evictIdle, Duration.ofSeconds(60));
    return r;
  }

  /** Spec §7.7: check cert and key every {@code tls.reload-interval}; new connections get the new certificate. */
  @Bean
  ScheduledFuture<?> tlsReload(GatewayProperties p, SslContextProvider tls, TickScheduler tick,
      ExecutorService offload) {
    return tick.scheduleAtFixedRate(() -> offload.execute(tls::reloadIfChanged), p.tls().reloadInterval());
  }

  /** TLS session ticket keys, required at start (like ticket-keys); installed on every context, now and reloaded. */
  @Bean
  AtomicReference<TlsTicketKeys> tlsTicketKeys(SecretSource secrets, SslContextProvider tls) {
    AtomicReference<TlsTicketKeys> keys = new AtomicReference<>(TlsTicketKeys.load(secrets));
    keys.get().applyTo(tls.ws());
    keys.get().applyTo(tls.tcp());
    tls.onContextsBuilt(ctx -> keys.get().applyTo(ctx));
    return keys;
  }

  /** Spec §4.7, §7.7: both key sets are re-read every 60 s; a broken read keeps the keys in use. */
  @Bean
  ScheduledFuture<?> keyReload(SecretSource secrets, SslContextProvider tls, AtomicReference<TlsTicketKeys> tlsKeys,
      ResumeTicketService tickets, TickScheduler tick, ExecutorService offload, GatewayMetrics metrics) {
    return tick.scheduleAtFixedRate(() -> offload.execute(() -> {
      reloadTlsTicketKeys(secrets, tls, tlsKeys, metrics);
      reloadTicketKeys(secrets, tickets, metrics);
    }), KEY_RELOAD);
  }

  static void reloadTlsTicketKeys(SecretSource secrets, SslContextProvider tls, AtomicReference<TlsTicketKeys> keys,
      GatewayMetrics metrics) {
    // Same monitor as SslContextProvider.reloadIfChanged: a rotation and a context swap never interleave, so new
    // contexts cannot keep the old keys.
    synchronized (tls) {
      rotateTlsTicketKeys(secrets, tls, keys, metrics);
    }
  }

  private static void rotateTlsTicketKeys(SecretSource secrets, SslContextProvider tls,
      AtomicReference<TlsTicketKeys> keys, GatewayMetrics metrics) {
    try {
      TlsTicketKeys next = TlsTicketKeys.load(secrets);
      if (!next.equals(keys.get())) {
        keys.set(next);
        next.applyTo(tls.ws());
        next.applyTo(tls.tcp());
        metrics.counter("gw.secrets.reload", "name", "tls-ticket-keys", "result", "ok").increment();
        log.info("TLS session ticket keys rotated");
      }
    } catch (RuntimeException e) {
      metrics.counter("gw.secrets.reload", "name", "tls-ticket-keys", "result", "failed").increment();
      log.warn("TLS ticket key reload failed, keeping the current keys: {}", e.getMessage());
    }
  }

  /** Public for tests in other packages; swaps the resume ticket ring when the key files changed. */
  public static void reloadTicketKeys(SecretSource secrets, ResumeTicketService tickets, GatewayMetrics metrics) {
    try {
      TicketKeyRing next = TicketKeyRing.load(secrets);
      if (!next.equals(tickets.ring())) {
        tickets.ring(next);
        metrics.counter("gw.secrets.reload", "name", "ticket-keys", "result", "ok").increment();
        log.info("resume ticket keys rotated (current kid {})", next.current().kid());
      }
    } catch (RuntimeException e) {
      metrics.counter("gw.secrets.reload", "name", "ticket-keys", "result", "failed").increment();
      log.warn("resume ticket key reload failed, keeping the current keys: {}", e.getMessage());
    }
  }
}
