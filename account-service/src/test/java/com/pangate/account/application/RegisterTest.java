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
  void sixthRegistrationFromOneIpInAnHourIs429() {
    for (int i = 0; i < 5; i++) {
      register.register("user" + i + "abc", "secret-pw".toCharArray(), "u" + i + "@x.vn", null, null, phone,
          "10.0.0.9");
    }
    assertThatThrownBy(() -> register.register("user9abc", "secret-pw".toCharArray(), "u9@x.vn", null, null, phone,
        "10.0.0.9")).isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code())
        .isEqualTo("rate_limited"));
    assertThat(register.register("user9abc", "secret-pw".toCharArray(), "u9@x.vn", null, null, phone, "10.0.0.10")
        .userId()).isNotNull();
  }

  @Test
  void deviceInfoIsChecked() {
    assertThatThrownBy(() -> DeviceInfo.of("x", "toaster")).isInstanceOf(DomainException.class);
    assertThatThrownBy(() -> DeviceInfo.of(" ", "ios")).isInstanceOf(DomainException.class);
  }
}
