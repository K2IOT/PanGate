package com.pangate.gateway.config;

import com.pangate.gateway.pipeline.SslContextProvider;
import com.pangate.gateway.server.TickScheduler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** M3b edge wiring: certificate and key reloads (file I/O on the offload executor; gw-tick only schedules). */
@Configuration(proxyBeanMethods = false)
public class EdgeConfig {

  /** Spec §7.7: check cert and key every {@code tls.reload-interval}; new connections get the new certificate. */
  @Bean
  ScheduledFuture<?> tlsReload(GatewayProperties p, SslContextProvider tls, TickScheduler tick,
      ExecutorService offload) {
    return tick.scheduleAtFixedRate(() -> offload.execute(tls::reloadIfChanged), p.tls().reloadInterval());
  }
}
