package com.pangate.account.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.account.application.port.Metrics;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.support.Fakes;
import com.pangate.account.support.TestLimits;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LoginUseCaseTest {

  final Fakes f = new Fakes();
  final InMemoryAttemptCounter counter = new InMemoryAttemptCounter(f.clock);
  final List<Duration> slept = new ArrayList<>();
  final Register register = new Register(f.accountStore, f.sessionStore, f.outbox, f.hasher, new HashGate(4),
      counter, TestLimits.SPEC, Set.of(), f.tx, Metrics.NONE);
  final LoginUseCase login = new LoginUseCase(f.accountStore, f.sessionStore, f.hasher, new HashGate(4), counter,
      TestLimits.SPEC, slept::add, f.tx, Metrics.NONE);
  final DeviceInfo phone = DeviceInfo.of("phone", "android");
  final NewSession lan = register.register("lannguyen", "secret-pw".toCharArray(), "lan@x.vn", "+84901234567", null,
      phone, "10.9.9.9");

  String code(String who, String pw, String ip) {
    try {
      login.login(who, pw.toCharArray(), null, phone, ip);
      return "ok";
    } catch (DomainException e) {
      return e.code();
    }
  }

  @Test
  void wrongLoginAndWrongPasswordLookTheSame() {
    assertThat(code("nobody", "secret-pw", "10.0.0.1")).isEqualTo("invalid_credentials");
    assertThat(f.burns.get()).isEqualTo(1);
    assertThat(code("lannguyen", "wrong-pw!", "10.0.0.1")).isEqualTo("invalid_credentials");
    assertThat(f.burns.get()).isEqualTo(1);
    assertThat(code("LanNguyen", "secret-pw", "10.0.0.1")).isEqualTo("ok");
    assertThat(code("+84901234567", "secret-pw", "10.0.0.1")).isEqualTo("ok");
    assertThat(code("LAN@x.vn", "secret-pw", "10.0.0.1")).isEqualTo("ok");
  }

  @Test
  void tenFailuresBlockThatLoginFromThatIpOnly() {
    for (int i = 0; i < 10; i++) {
      assertThat(code("lannguyen", "wrong-pw!", "10.0.0.1")).isEqualTo("invalid_credentials");
    }
    assertThat(code("lannguyen", "secret-pw", "10.0.0.1")).isEqualTo("too_many_attempts");
    assertThat(code("lannguyen", "secret-pw", "10.0.0.2")).isEqualTo("ok");
    f.now = f.now.plus(Duration.ofMinutes(16));
    assertThat(code("lannguyen", "secret-pw", "10.0.0.1")).isEqualTo("ok");
  }

  @Test
  void fiftyFailuresBlockTheIp() {
    for (int i = 0; i < 50; i++) {
      assertThat(code("nobody" + i, "wrong-pw!", "10.0.0.3")).isEqualTo("invalid_credentials");
    }
    assertThat(code("lannguyen", "secret-pw", "10.0.0.3")).isEqualTo("too_many_attempts");
  }

  @Test
  void hundredFailuresFromManyIpsSlowTheLoginDown() {
    for (int i = 0; i < 100; i++) {
      code("lannguyen", "wrong-pw!", "10.1." + (i / 200) + "." + (i % 200));
    }
    assertThat(slept).isEmpty();
    assertThat(code("lannguyen", "secret-pw", "10.2.0.1")).isEqualTo("ok");
    assertThat(slept).containsExactly(Duration.ofSeconds(2));
  }

  @Test
  void lockedAccountIs403AndDeletedLooksUnknown() {
    f.accountStore.setStatus(lan.userId(), AccountStatus.LOCKED);
    assertThat(code("lannguyen", "wrong-pw!", "10.0.0.4")).isEqualTo("invalid_credentials");
    assertThat(code("lannguyen", "secret-pw", "10.0.0.4")).isEqualTo("account_locked");
    f.accountStore.setStatus(lan.userId(), AccountStatus.ACTIVE);
    f.accountStore.erase(lan.userId());
    assertThat(code("lannguyen", "secret-pw", "10.0.0.4")).isEqualTo("invalid_credentials");
  }

  @Test
  void mustChangePassword() {
    f.accountStore.setPassword(lan.userId(), "h:temp-pass-1", true);
    assertThat(code("lannguyen", "temp-pass-1", "10.0.0.5")).isEqualTo("password_change_required");
    assertThatThrownBy(() -> login.login("lannguyen", "temp-pass-1".toCharArray(), "short".toCharArray(), phone,
        "10.0.0.5")).isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code())
        .isEqualTo("weak_password"));
    NewSession s = login.login("lannguyen", "temp-pass-1".toCharArray(), "brand-new-pw".toCharArray(), phone,
        "10.0.0.5");
    assertThat(s.userId()).isEqualTo(lan.userId());
    assertThat(f.accounts.get(lan.userId()).passwordHash()).isEqualTo("h:brand-new-pw");
    assertThat(f.accounts.get(lan.userId()).mustChangePassword()).isFalse();
  }
}
