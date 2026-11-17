package com.pangate.account.config;

import com.pangate.account.application.ExpireSessions;
import com.pangate.account.application.RevocationRelay;
import com.pangate.account.application.port.AuthApi;
import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.infrastructure.authclient.HttpAuthApi;
import com.pangate.account.infrastructure.kafka.EventRelay;
import com.pangate.account.infrastructure.postgres.AdvisoryLock;
import com.pangate.starter.secrets.SecretSource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.transaction.support.TransactionTemplate;

/** auth-service client, the two outbox relays, session expiry and the gauges of account spec §6, §7. */
@Configuration(proxyBeanMethods = false)
public class JobsConfig {

  private static final Logger log = LoggerFactory.getLogger(JobsConfig.class);

  @Bean
  AuthApi authApi(AccountProperties p, SecretSource secrets) {
    return new HttpAuthApi(p.auth().baseUrl(), p.auth().clientId(), AuthClientSecret.load(p.auth(), secrets),
        p.auth().timeout());
  }

  @Bean
  AdvisoryLock advisoryLock(TransactionTemplate tx, JdbcClient jdbc) {
    return new AdvisoryLock(tx, jdbc);
  }

  @Bean
  RevocationRelay revocationRelay(Outbox outbox, AuthApi auth, AccountProperties p, Metrics metrics) {
    return new RevocationRelay(outbox, auth, p.outbox().batch(), metrics);
  }

  @Bean(destroyMethod = "close")
  EventRelay eventRelay(Outbox outbox, AccountProperties p) {
    return new EventRelay(outbox, EventRelay.producer(p.kafka().bootstrap(), Duration.ofSeconds(2)),
        p.kafka().usersTopic(), p.outbox().batch(), Duration.ofSeconds(2));
  }

  @Bean
  ExpireSessions expireSessions(SessionStore sessions, Outbox outbox, AccountProperties p) {
    return new ExpireSessions(sessions, outbox, Clock.SYSTEM, p.sessionIdleTtl(), p.outbox().keepSent());
  }

  static Runnable guarded(String what, Runnable r) {
    return () -> {
      try {
        r.run();
      } catch (RuntimeException e) {
        log.warn("{}: {}", what, e.toString());
      }
    };
  }

  /** Each pass under its own advisory lock, so one pod at a time runs each job; the relays never wait on each other. */
  @Bean
  SchedulingConfigurer accountJobs(AdvisoryLock lock, EventRelay events, RevocationRelay revocations,
      ExpireSessions expire, AccountProperties p, Outbox outbox, SessionStore sessions, MeterRegistry registry) {
    AtomicLong liveSessions = new AtomicLong();
    java.util.Map<String, AtomicLong[]> pendingGauges = new java.util.HashMap<>();
    for (String kind : new String[] {Outbox.EVENT, Outbox.REVOCATION}) {
      AtomicLong pending = new AtomicLong();
      AtomicLong oldestMs = new AtomicLong();
      Gauge.builder("account.outbox.pending", pending, AtomicLong::get).tag("kind", kind).register(registry);
      Gauge.builder("account.outbox.oldest_age_seconds", oldestMs, v -> v.get() / 1000.0).tag("kind", kind)
          .register(registry);
      registry.config();
      pendingGauges.put(kind, new AtomicLong[] {pending, oldestMs});
    }
    Gauge.builder("account.sessions.active", liveSessions, AtomicLong::get).register(registry);
    return (ScheduledTaskRegistrar r) -> {
      // one thread per job: with the default single thread a Kafka outage (2 s per send) would hold the revocations
      r.setScheduler(java.util.concurrent.Executors.newScheduledThreadPool(5,
          Thread.ofPlatform().daemon().name("account-job-", 0).factory()));
      r.addFixedDelayTask(guarded("event relay",
          () -> lock.runExclusive("account.event-relay", events::runOnce)), p.outbox().interval());
      r.addFixedDelayTask(guarded("revocation relay",
          () -> lock.runExclusive("account.revocation-relay", revocations::runOnce)), p.outbox().interval());
      r.addFixedDelayTask(guarded("session expiry",
          () -> lock.runExclusive("account.expire-sessions", expire::runOnce)), Duration.ofHours(1));
      r.addFixedDelayTask(guarded("outbox gauges", () -> pendingGauges.forEach((kind, g) -> {
        g[0].set(outbox.pendingCount(kind));
        g[1].set((long) (outbox.oldestAgeSeconds(kind) * 1000));
      })), Duration.ofSeconds(10));
      r.addFixedDelayTask(guarded("session gauge", () -> liveSessions.set(sessions.countLive())),
          Duration.ofSeconds(60));
    };
  }

}
