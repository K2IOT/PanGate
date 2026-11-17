package com.pangate.notify.config;

import com.pangate.notify.api.ConnectAuthFilter;
import com.pangate.notify.application.Registrations;
import com.pangate.notify.application.port.RateLimiter;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.infrastructure.auth.ConnectTokenVerifier;
import com.pangate.notify.infrastructure.auth.HttpJwksSource;
import com.pangate.notify.infrastructure.auth.RevocationListener;
import com.pangate.notify.infrastructure.auth.Revocations;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Connect tokens on the public API (offline notifications spec §5.1): JWKS of auth-service and the revocations of
 * {@code gw.control}, read from the start by every pod; and the registration use case.
 */
@Configuration(proxyBeanMethods = false)
public class AuthConfig {

  static long monoMs() {
    return System.nanoTime() / 1_000_000;
  }

  @Bean
  Revocations revocations() {
    return new Revocations(System::currentTimeMillis);
  }

  @Bean(destroyMethod = "")
  RevocationListener revocationListener(NotifyProperties p, Revocations revocations, MeterRegistry meters) {
    KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.kafka().bootstrap(),
        ConsumerConfig.CLIENT_ID_CONFIG, "notify-control-" + UUID.randomUUID(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new ByteArrayDeserializer());
    return new RevocationListener(c, p.kafka().controlTopic(), revocations,
        () -> meters.counter("notify.control.records", "result", "malformed").increment());
  }

  @Bean
  ConnectTokenVerifier connectTokenVerifier(NotifyProperties p, Revocations revocations) {
    return new ConnectTokenVerifier(new HttpJwksSource(p.auth().jwksUri()), p.auth().issuer(), "pangate-gw",
        revocations, System::currentTimeMillis, AuthConfig::monoMs);
  }

  @Bean
  FilterRegistrationBean<ConnectAuthFilter> connectAuthRegistration(ConnectTokenVerifier verifier) {
    FilterRegistrationBean<ConnectAuthFilter> r = new FilterRegistrationBean<>(new ConnectAuthFilter(verifier));
    r.setOrder(10);
    r.addUrlPatterns("/v1/*");
    return r;
  }

  /** Readiness: the revocations of gw.control were read to the end and a JWKS was loaded. */
  @Bean
  HealthIndicator connectAuthHealthIndicator(RevocationListener listener, ConnectTokenVerifier verifier) {
    return () -> listener.caughtUp() && verifier.loaded() ? Health.up().build()
        : Health.down().withDetail("revocationsCaughtUp", listener.caughtUp())
            .withDetail("jwksLoaded", verifier.loaded()).build();
  }

  @Bean
  SmartLifecycle connectAuthBackground(RevocationListener listener, ConnectTokenVerifier verifier) {
    return new SmartLifecycle() {
      private volatile boolean running;
      private ScheduledExecutorService timers;

      @Override
      public void start() {
        timers = Executors.newScheduledThreadPool(1, Thread.ofPlatform().daemon().name("notify-jwks").factory());
        timers.scheduleWithFixedDelay(() -> verifier.refresh(true), 0, 300, TimeUnit.SECONDS);
        timers.scheduleWithFixedDelay(() -> {
          if (!verifier.loaded()) {
            verifier.refresh(false);
          }
        }, 2, 2, TimeUnit.SECONDS);
        listener.start();
        running = true;
      }

      @Override
      public void stop() {
        running = false;
        listener.close();
        timers.shutdownNow();
      }

      @Override
      public boolean isRunning() {
        return running;
      }
    };
  }

  @Bean
  Registrations registrations(TokenStore store, RateLimiter limiter, NotifyProperties p, MeterRegistry meters) {
    return new Registrations(store, limiter, p.registrationsPerMinute(), Clock.systemUTC(), meters);
  }
}
