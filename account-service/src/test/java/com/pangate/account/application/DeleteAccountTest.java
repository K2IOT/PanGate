package com.pangate.account.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.application.port.Metrics;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.support.Fakes;
import com.pangate.account.support.TestLimits;
import com.pangate.contracts.v1.AccountState;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DeleteAccountTest {

  final Fakes f = new Fakes();

  @Test
  void deleteRevokesThePrincipalAndPublishesDeleted() {
    NewSession s = new Register(f.accountStore, f.sessionStore, f.outbox, f.hasher, new HashGate(2),
        new InMemoryAttemptCounter(f.clock), TestLimits.SPEC, Set.of(), f.tx, Metrics.NONE).register("lannguyen",
        "secret-pw".toCharArray(), "l@x.vn", null, null, DeviceInfo.of("p", "ios"), "1.1.1.1");
    new DeleteAccount(f.accountStore, f.sessionStore, f.outbox, null, f.tx, f.clock).delete(s.userId());
    assertThat(f.accounts.get(s.userId()).status()).isEqualTo(AccountStatus.DELETED);
    assertThat(f.accounts.get(s.userId()).email()).isNull();
    assertThat(f.sessionStore.live(s.userId())).isEmpty();
    assertThat(f.revocations(s.userId())).containsExactly("device:" + s.deviceId(), "principal");
    assertThat(f.events(s.userId())).containsExactly(AccountState.Status.STATUS_ACTIVE,
        AccountState.Status.STATUS_DELETED);
  }
}
