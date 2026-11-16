package com.pangate.account.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.account.application.port.Metrics;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.LoginKind;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.support.Fakes;
import com.pangate.account.support.TestLimits;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ChangeIdentifierTest {

  final Fakes f = new Fakes();
  final InMemoryAttemptCounter counter = new InMemoryAttemptCounter(f.clock);
  final HashGate gate = new HashGate(2);
  final LoginUseCase login = new LoginUseCase(f.accountStore, f.sessionStore, f.hasher, gate, counter,
      TestLimits.SPEC, d -> { }, f.tx, Metrics.NONE);
  final ChangeIdentifier change = new ChangeIdentifier(f.accountStore, login, f.tx);
  final NewSession s = new Register(f.accountStore, f.sessionStore, f.outbox, f.hasher, gate, counter,
      TestLimits.SPEC, Set.of(), f.tx, Metrics.NONE).register("lannguyen", "secret-pw".toCharArray(), "l@x.vn", null,
      null, DeviceInfo.of("p", "ios"), "1.1.1.1");
  final Caller me = new Caller(s.userId(), s.deviceId(), "1.1.1.1");

  @Test
  void cannotRemoveTheLastIdentifier() {
    assertThatThrownBy(() -> change.set(me, LoginKind.EMAIL, null, "secret-pw".toCharArray()))
        .isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code()).isEqualTo("invalid_request"));
    change.set(me, LoginKind.PHONE, " +84901234567 ", "secret-pw".toCharArray());
    change.set(me, LoginKind.EMAIL, null, "secret-pw".toCharArray());
    assertThat(f.accounts.get(s.userId()).email()).isNull();
    assertThat(f.accounts.get(s.userId()).phone()).isEqualTo("+84901234567");
  }

  @Test
  void wrongPasswordIs401() {
    assertThatThrownBy(() -> change.set(me, LoginKind.EMAIL, "new@x.vn", "wrong-pw!".toCharArray()))
        .isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code()).isEqualTo("invalid_credentials"));
    assertThat(f.accounts.get(s.userId()).email()).isEqualTo("l@x.vn");
  }
}
