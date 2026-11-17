package com.pangate.account.config;

import com.pangate.account.application.LoginUseCase;
import com.pangate.account.application.Register;
import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.PasswordHasher;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Sleeper;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.infrastructure.crypto.Argon2PasswordHasher;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.infrastructure.redis.RedisAttemptCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Use cases and their adapters (account spec §2–§7). */
@Configuration(proxyBeanMethods = false)
public class AccountConfig {

  @Bean
  PasswordHasher passwordHasher() {
    return new Argon2PasswordHasher();
  }

  @Bean
  HashGate hashGate(AccountProperties p, MeterRegistry registry) {
    return new HashGate(HashGate.permits(p.maxConcurrentHashes()), new HashGate.Observer() {
      @Override
      public void took(java.time.Duration d) {
        registry.timer("account.password.hash.latency").record(d);
      }

      @Override
      public void rejected() {
        registry.counter("account.password.hash.rejected").increment();
      }
    });
  }

  /** Shared through Redis; per pod (its in-memory fallback) while Redis cannot be used. */
  @Bean(destroyMethod = "close")
  RedisAttemptCounter attemptCounter(AccountProperties p) {
    return new RedisAttemptCounter(p.redis().uri(), p.redis().timeout(), new InMemoryAttemptCounter(Clock.SYSTEM));
  }

  @Bean
  Metrics metrics(MeterRegistry registry) {
    return (name, tags) -> registry.counter(name, tags).increment();
  }

  static Set<String> reserved(AccountProperties p) {
    Set<String> out = new HashSet<>();
    p.reservedUsernames().forEach(n -> out.add(n.toLowerCase(Locale.ROOT)));
    return out;
  }

  @Bean
  Register register(AccountStore accounts, SessionStore sessions, Outbox outbox, PasswordHasher hasher,
      HashGate gate, AttemptCounter counter, AccountProperties p, Transactions tx, Metrics metrics) {
    return new Register(accounts, sessions, outbox, hasher, gate, counter, p.limits(), reserved(p), tx, metrics);
  }

  @Bean
  com.pangate.account.application.Profiles profiles(AccountStore accounts, AttemptCounter counter,
      AccountProperties p, com.pangate.account.application.port.Outbox outbox,
      com.pangate.account.application.port.Transactions tx) {
    return new com.pangate.account.application.Profiles(accounts, reserved(p), counter, p.limits(), outbox, tx);
  }

  @Bean
  com.pangate.account.application.ChangePassword changePassword(AccountStore accounts, SessionStore sessions,
      Outbox outbox, PasswordHasher hasher, HashGate gate, LoginUseCase login, Transactions tx) {
    return new com.pangate.account.application.ChangePassword(accounts, sessions, outbox, hasher, gate, login, tx);
  }

  @Bean
  com.pangate.account.application.ChangeIdentifier changeIdentifier(AccountStore accounts, LoginUseCase login,
      Transactions tx) {
    return new com.pangate.account.application.ChangeIdentifier(accounts, login, tx);
  }

  @Bean
  LoginUseCase loginUseCase(AccountStore accounts, SessionStore sessions, PasswordHasher hasher, HashGate gate,
      AttemptCounter counter, AccountProperties p, Transactions tx, Metrics metrics) {
    return new LoginUseCase(accounts, sessions, hasher, gate, counter, p.limits(), Sleeper.REAL, tx, metrics);
  }
}
