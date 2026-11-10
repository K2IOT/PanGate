package com.pangate.auth.config;

import com.pangate.auth.application.KeyRing;
import com.pangate.auth.application.RotateSigningKeys;
import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.KeyCipher;
import com.pangate.auth.application.port.SecretHasher;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.infrastructure.crypto.AesGcmKeyCipher;
import com.pangate.auth.infrastructure.crypto.Argon2SecretHasher;
import com.pangate.auth.infrastructure.crypto.Es256TokenSigner;
import com.pangate.auth.infrastructure.crypto.Kek;
import com.pangate.auth.infrastructure.postgres.AdvisoryLock;
import com.pangate.auth.infrastructure.postgres.JdbcSigningKeyStore;
import com.pangate.starter.secrets.SecretSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.transaction.support.TransactionTemplate;

/** Crypto adapters, key store, key ring and the key jobs (spec §3, §5). */
@Configuration(proxyBeanMethods = false)
public class JobsConfig {

  private static final Logger log = LoggerFactory.getLogger(JobsConfig.class);
  static final String ROTATE_LOCK = "auth.rotate-keys";

  @Bean
  Clock authClock() {
    return Instant::now;
  }

  @Bean
  KeyCipher keyCipher(SecretSource secrets, AuthProperties props, JdbcClient jdbc) {
    Map<String, Kek> keks = new LinkedHashMap<>();
    String current = props.keys().kekId();
    keks.put(current, Kek.load(secrets, current));
    for (String id : jdbc.sql("SELECT DISTINCT kek_id FROM auth.signing_keys").query(String.class).list()) {
      keks.computeIfAbsent(id, k -> Kek.load(secrets, k));
    }
    return new AesGcmKeyCipher(keks, current);
  }

  @Bean
  SecretHasher secretHasher() {
    return new Argon2SecretHasher();
  }

  @Bean
  TokenSigner tokenSigner(AuthProperties props) {
    return new Es256TokenSigner(props.issuer());
  }

  @Bean
  JdbcSigningKeyStore signingKeyStore(JdbcClient jdbc, KeyCipher cipher, MeterRegistry metrics) {
    return new JdbcSigningKeyStore(jdbc, cipher, metrics);
  }

  @Bean
  AdvisoryLock advisoryLock(TransactionTemplate tx, JdbcClient jdbc) {
    return new AdvisoryLock(tx, jdbc);
  }

  @Bean
  RotateSigningKeys rotateSigningKeys(JdbcSigningKeyStore store, Clock clock, AuthProperties props) {
    return new RotateSigningKeys(store, clock, props.keys().policy(), Es256TokenSigner::newKey);
  }

  @Bean
  KeyRing keyRing(JdbcSigningKeyStore store) {
    return new KeyRing(store);
  }

  @Bean
  KeyJobs keyJobs(RotateSigningKeys rotate, AdvisoryLock lock, KeyRing ring) {
    return new KeyJobs(rotate, lock, ring);
  }

  /** Rotation once before the app is ready, so a fresh database has a signing key at once. */
  @Bean
  ApplicationRunner initialKeyRotation(KeyJobs jobs) {
    return args -> jobs.rotate();
  }

  @Bean
  SchedulingConfigurer keySchedules(KeyJobs jobs, AuthProperties props) {
    return (ScheduledTaskRegistrar r) -> {
      r.addFixedDelayTask(jobs::rotate, props.keys().rotateInterval());
      r.addFixedDelayTask(jobs.ring::reload, props.keys().cacheRefresh());
    };
  }

  /** Key jobs, each pass under the advisory lock and its own transaction. */
  public static final class KeyJobs {
    private final RotateSigningKeys rotate;
    private final AdvisoryLock lock;
    final KeyRing ring;

    KeyJobs(RotateSigningKeys rotate, AdvisoryLock lock, KeyRing ring) {
      this.rotate = rotate;
      this.lock = lock;
      this.ring = ring;
    }

    /**
     * One rotation pass, then a key-ring reload. Passes of this pod run one at a time. If another pod holds the lock
     * while this pod has no signing key yet (several pods starting on an empty database), waits for it briefly so
     * this pod can sign as soon as it is ready. Never throws.
     */
    private final java.util.concurrent.locks.ReentrantLock passes = new java.util.concurrent.locks.ReentrantLock();

    public void rotate() {
      passes.lock();  // a ReentrantLock, not synchronized: the pass sleeps and does I/O on a virtual thread
      try {
        rotateLocked();
      } finally {
        passes.unlock();
      }
    }

    private void rotateLocked() {
      for (int attempt = 0; attempt < 25; attempt++) {
        boolean[] changed = {false};
        boolean ran = false;
        try {
          ran = lock.runExclusive(ROTATE_LOCK, () -> changed[0] = rotate.runOnce());
        } catch (RuntimeException e) {
          log.warn("key rotation pass failed (another pod may have rotated): {}", e.toString());
        }
        ring.reload();
        if (changed[0]) {
          log.info("signing keys rotated; {} keys published", ring.size());
        }
        if (ran || ring.canSign()) {
          return;
        }
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    /** Admin {@code /keys:rotate-now}. */
    public boolean forcePending() {
      passes.lock();
      try {
        boolean[] created = {false};
        lock.runExclusive(ROTATE_LOCK, () -> created[0] = rotate.forcePending());
        ring.reload();
        return created[0];
      } finally {
        passes.unlock();
      }
    }
  }
}
