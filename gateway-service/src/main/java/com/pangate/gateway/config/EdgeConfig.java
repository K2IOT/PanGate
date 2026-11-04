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
