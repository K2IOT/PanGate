package com.pangate.notify.config;

import com.pangate.notify.application.PurgeIdleTokens;
import com.pangate.notify.application.TokenRevoker;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.infrastructure.kafka.RevocationDeleter;
import com.pangate.notify.infrastructure.postgres.AdvisoryLock;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Token removal on revocations (spec §5.3) and the daily purge of idle tokens (spec §5.1). */
@Configuration(proxyBeanMethods = false)
public class JobsConfig {

  private static final Logger log = LoggerFactory.getLogger(JobsConfig.class);

  @Bean
  TokenRevoker tokenRevoker(TokenStore store, MeterRegistry meters) {
    return new TokenRevoker(store, meters);
  }

  @Bean(destroyMethod = "")
  RevocationDeleter revocationDeleter(NotifyProperties p, TokenRevoker revoker, MeterRegistry meters) {
    return new RevocationDeleter(RevocationDeleter.consumer(p.kafka().bootstrap()), p.kafka().controlTopic(), revoker,
        () -> meters.counter("notify.control.records", "result", "malformed").increment());
  }

  @Bean
  SmartLifecycle revocationDeleterLifecycle(RevocationDeleter deleter) {
    return new SmartLifecycle() {
      private volatile boolean running;

      @Override
      public void start() {
        deleter.start();
        running = true;
      }

      @Override
      public void stop() {
        running = false;
        deleter.close();
      }

      @Override
      public boolean isRunning() {
        return running;
      }
    };
  }

  @Bean
  AdvisoryLock advisoryLock(PlatformTransactionManager tm, JdbcClient jdbc) {
    return new AdvisoryLock(new TransactionTemplate(tm), jdbc);
  }

  @Bean
  PurgeIdleTokens purgeIdleTokens(TokenStore store, NotifyProperties p) {
    return new PurgeIdleTokens(store, p.tokenIdleTtl(), Clock.systemUTC());
  }

  /** Once a day, on one pod at a time. */
  @Bean
  SchedulingConfigurer notifyJobs(AdvisoryLock lock, PurgeIdleTokens purge, MeterRegistry meters) {
    return r -> r.addFixedDelayTask(() -> {
      try {
        lock.runExclusive("notify-purge-idle", () -> {
          int n = purge.run();
          meters.counter("notify.tokens.removed", "reason", "idle").increment(n);
          if (n > 0) {
            log.info("purged {} idle push tokens", n);
          }
        });
      } catch (RuntimeException e) {
        log.warn("idle token purge failed: {}", e.toString());
      }
    }, Duration.ofDays(1));
  }
}
