package com.pangate.account.config;

import com.pangate.account.api.ConnectAuthFilter;
import com.pangate.account.application.ExchangeToken;
import com.pangate.account.application.ManageSessions;
import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.application.port.AuthApi;
import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.infrastructure.auth.ConnectTokenVerifier;
import com.pangate.account.infrastructure.auth.HttpJwksSource;
import com.pangate.account.infrastructure.auth.RevocationListener;
import com.pangate.account.infrastructure.auth.Revocations;
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
 * Connect tokens on the public API (account spec §2): JWKS of auth-service, revocations from {@code gw.control}
 * (read from the start, like chat-service), the session check; and the token exchange and session use cases.
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
  RevocationListener revocationListener(AccountProperties p, Revocations revocations, Metrics metrics) {
    KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.kafka().bootstrap(),
        ConsumerConfig.CLIENT_ID_CONFIG, "account-control-" + UUID.randomUUID(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new ByteArrayDeserializer());
    return new RevocationListener(c, p.kafka().controlTopic(), revocations,
        () -> metrics.count("account.control.records", "result", "malformed"));
  }

  @Bean
  ConnectTokenVerifier connectTokenVerifier(AccountProperties p, Revocations revocations) {
    return new ConnectTokenVerifier(new HttpJwksSource(p.auth().jwksUri()), p.auth().issuer(), "pangate-gw",
        revocations, System::currentTimeMillis, AuthConfig::monoMs);
  }

  @Bean
  ConnectAuthFilter connectAuthFilter(ConnectTokenVerifier verifier, SessionStore sessions) {
    return new ConnectAuthFilter(verifier, sessions, AuthConfig::monoMs);
  }

  @Bean
  FilterRegistrationBean<ConnectAuthFilter> connectAuthRegistration(ConnectAuthFilter f) {
    FilterRegistrationBean<ConnectAuthFilter> r = new FilterRegistrationBean<>(f);
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
        timers = Executors.newScheduledThreadPool(1, Thread.ofPlatform().daemon().name("account-jwks").factory());
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
  com.pangate.account.infrastructure.auth.ApiTokenVerifier apiTokenVerifier(AccountProperties p,
      Revocations revocations) {
    return new com.pangate.account.infrastructure.auth.ApiTokenVerifier(new HttpJwksSource(p.auth().jwksUri()),
        p.auth().issuer(), revocations, System::currentTimeMillis, AuthConfig::monoMs);
  }

  @Bean
  FilterRegistrationBean<com.pangate.account.api.AdminAuthFilter> adminAuthRegistration(
      com.pangate.account.infrastructure.auth.ApiTokenVerifier verifier, AttemptCounter counter,
      AccountProperties p) {
    FilterRegistrationBean<com.pangate.account.api.AdminAuthFilter> r = new FilterRegistrationBean<>(
        new com.pangate.account.api.AdminAuthFilter(verifier, p.auth().adminTenant(), counter,
            p.limits().adminPerSecond()));
    r.setOrder(10);
    r.addUrlPatterns("/admin/v1/*");
    return r;
  }

  @Bean
  com.pangate.account.application.DeleteAccount deleteAccount(AccountStore accounts, SessionStore sessions,
      Outbox outbox, com.pangate.account.application.LoginUseCase login, Transactions tx) {
    return new com.pangate.account.application.DeleteAccount(accounts, sessions, outbox, login, tx, Clock.SYSTEM);
  }

  @Bean
  com.pangate.account.application.AdminAccounts adminAccounts(AccountStore accounts, SessionStore sessions,
      Outbox outbox, com.pangate.account.application.port.AdminLog log,
      com.pangate.account.application.port.PasswordHasher hasher,
      com.pangate.account.infrastructure.crypto.HashGate gate,
      com.pangate.account.application.DeleteAccount deleter, Transactions tx, Metrics metrics) {
    return new com.pangate.account.application.AdminAccounts(accounts, sessions, outbox, log, hasher, gate, deleter,
        tx, Clock.SYSTEM, metrics);
  }

  @Bean
  ExchangeToken exchangeToken(SessionStore sessions, AccountStore accounts, AuthApi auth, AttemptCounter counter,
      AccountProperties p, Metrics metrics) {
    return new ExchangeToken(sessions, accounts, auth, counter, p.limits(), Clock.SYSTEM, p.sessionIdleTtl(),
        p.lastUsedWriteEvery(), metrics);
  }

  @Bean
  ManageSessions manageSessions(SessionStore sessions, Outbox outbox, Transactions tx) {
    return new ManageSessions(sessions, outbox, tx);
  }
}
