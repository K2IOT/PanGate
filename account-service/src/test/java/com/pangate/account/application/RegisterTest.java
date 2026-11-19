package com.pangate.account.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.account.application.port.Metrics;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.support.Fakes;
import com.pangate.account.support.TestLimits;
import com.pangate.contracts.v1.AccountState;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RegisterTest {

  final Fakes f = new Fakes();
  final Register register = new Register(f.accountStore, f.sessionStore, f.outbox, f.hasher, new HashGate(4),
      new InMemoryAttemptCounter(f.clock), TestLimits.SPEC, Set.of("admin"), f.tx, Metrics.NONE);
  final DeviceInfo phone = DeviceInfo.of("Lan's phone", "ios");

  @Test
  void needsEmailOrPhone() {
    assertThatThrownBy(() -> register.register("lannguyen", "secret-pw".toCharArray(), null, null, null, phone,
        "10.0.0.1")).isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code())
        .isEqualTo("invalid_request"));
  }

  @Test
  void registrationCreatesAccountSessionAndActiveState() {
    NewSession s = register.register("LanNguyen", "secret-pw".toCharArray(), " Lan@Mail.com ", null, null, phone,
        "10.0.0.1");
    assertThat(f.accounts.get(s.userId())).satisfies(a -> {
      assertThat(a.username()).isEqualTo("LanNguyen");
      assertThat(a.displayName()).isEqualTo("LanNguyen");
      assertThat(a.email()).isEqualTo("lan@mail.com");
      assertThat(a.passwordHash()).isEqualTo("h:secret-pw");
    });
    assertThat(f.sessions.get(s.deviceId()).userId()).isEqualTo(s.userId());
    assertThat(s.refreshToken()).startsWith("rt_");
    assertThat(f.events(s.userId())).containsExactly(AccountState.Status.STATUS_ACTIVE);
  }

  @Test
  void oneRegistrationOverTheLimitFromOneIpInAnHourIs429() {
    for (int i = 0; i < TestLimits.SPEC.registrationsPerIpPerHour(); i++) {
      register.register("user" + i + "abc", "secret-pw".toCharArray(), "u" + i + "@x.vn", null, null, phone,
          "10.0.0.9");
    }
    assertThatThrownBy(() -> register.register("overlimit", "secret-pw".toCharArray(), "over@x.vn", null, null,
        phone, "10.0.0.9")).isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code())
        .isEqualTo("rate_limited"));
    assertThat(register.register("overlimit", "secret-pw".toCharArray(), "over@x.vn", null, null, phone,
        "10.0.0.10").userId()).isNotNull();
  }

  @Test
  void deviceInfoIsChecked() {
    assertThatThrownBy(() -> DeviceInfo.of("x", "toaster")).isInstanceOf(DomainException.class);
    assertThatThrownBy(() -> DeviceInfo.of(" ", "ios")).isInstanceOf(DomainException.class);
  }
  /** Final review I8 (ruling 31 reversed): only registrations that succeed count toward the per-IP hourly limit. */
  @Test
  void failedRegistrationsDoNotCountTowardTheLimit() {
    register.register("taken1x", "secret-pw".toCharArray(), "t@x.vn", null, null, phone, "10.0.0.7");
    for (int i = 0; i < 6; i++) {
      assertThatThrownBy(() -> register.register("taken1x", "secret-pw".toCharArray(), "o@x.vn", null, null, phone,
          "10.0.0.7")).isInstanceOf(DomainException.class);
    }
    assertThat(register.register("fresh1x", "secret-pw".toCharArray(), "f@x.vn", null, null, phone, "10.0.0.7")
        .userId()).isNotNull();
  }

  /** Review round 2, Important 1: every attempt counts toward a looser cap, so "taken" answers are bounded. */
  @Test
  void takenAnswersStopAtTheAttemptCap() {
    register.register("taken2x", "secret-pw".toCharArray(), "t2@x.vn", null, null, phone, "10.0.0.6");
    for (int i = 1; i < TestLimits.SPEC.registrationAttemptsPerIpPerHour(); i++) {
      assertThatThrownBy(() -> register.register("other2x", "secret-pw".toCharArray(), "t2@x.vn", null, null, phone,
          "10.0.0.6")).isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code())
          .isEqualTo("email_taken"));
    }
    assertThatThrownBy(() -> register.register("other2x", "secret-pw".toCharArray(), "t2@x.vn", null, null, phone,
        "10.0.0.6")).isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code())
        .isEqualTo("rate_limited"));
  }

  /** Review round 2, Important 1: a burst that passes the cheap check together still creates at most the limit. */
  @Test
  void parallelRegistrationsFromOneIpCreateAtMostTheLimit() throws Exception {
    int n = TestLimits.SPEC.registrationsPerIpPerHour() + 5;
    java.util.concurrent.CyclicBarrier together = new java.util.concurrent.CyclicBarrier(n);
    com.pangate.account.application.port.PasswordHasher slow = new com.pangate.account.application.port
        .PasswordHasher() {
      @Override
      public String hash(char[] password) {
        try {
          together.await(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
        return f.hasher.hash(password);
      }

      @Override
      public boolean matches(char[] password, String hash) {
        return f.hasher.matches(password, hash);
      }

      @Override
      public void burn() {}
    };
    Register r = new Register(f.accountStore, f.sessionStore, f.outbox, slow, new HashGate(n),
        new InMemoryAttemptCounter(f.clock), TestLimits.SPEC, Set.of(), f.tx, Metrics.NONE);
    java.util.concurrent.atomic.AtomicInteger created = new java.util.concurrent.atomic.AtomicInteger();
    try (var pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
      java.util.List<java.util.concurrent.Future<?>> all = new java.util.ArrayList<>();
      for (int i = 0; i < n; i++) {
        int k = i;
        all.add(pool.submit(() -> {
          try {
            r.register("burst" + k + "x", "secret-pw".toCharArray(), "b" + k + "@x.vn", null, null, phone,
                "10.0.0.5");
            created.incrementAndGet();
          } catch (DomainException e) {
            assertThat(e.code()).isEqualTo("rate_limited");
          }
        }));
      }
      for (var x : all) {
        x.get();
      }
    }
    assertThat(created.get()).isEqualTo(TestLimits.SPEC.registrationsPerIpPerHour());
  }
}
