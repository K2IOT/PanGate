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
import com.pangate.contracts.v1.AccountState;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AdminAccountsTest {

  final Fakes f = new Fakes();
  final HashGate gate = new HashGate(2);
  final InMemoryAttemptCounter counter = new InMemoryAttemptCounter(f.clock);
  final Register register = new Register(f.accountStore, f.sessionStore, f.outbox, f.hasher, gate, counter,
      TestLimits.SPEC, Set.of(), f.tx, Metrics.NONE);
  final DeleteAccount delete = new DeleteAccount(f.accountStore, f.sessionStore, f.outbox, null, f.tx, f.clock);
  final AdminAccounts admin = new AdminAccounts(f.accountStore, f.sessionStore, f.outbox, f.adminLog, f.hasher, gate,
      delete, f.tx, f.clock, Metrics.NONE);
  final NewSession s = register.register("lannguyen", "secret-pw".toCharArray(), "l@x.vn", null, null,
      DeviceInfo.of("p", "ios"), "1.1.1.1");
  final String d2 = new LoginUseCase(f.accountStore, f.sessionStore, f.hasher, gate, counter, TestLimits.SPEC,
      d -> { }, f.tx, Metrics.NONE).login("lannguyen", "secret-pw".toCharArray(), null, DeviceInfo.of("q", "web"),
      "1.1.1.1").deviceId();

  @Test
  void reasonIsRequired() {
    for (String reason : new String[] {null, " "}) {
      assertThatThrownBy(() -> admin.lock("ops", s.userId(), reason)).isInstanceOfSatisfying(DomainException.class,
          e -> assertThat(e.code()).isEqualTo("invalid_request"));
    }
    assertThat(f.accounts.get(s.userId()).status()).isEqualTo(AccountStatus.ACTIVE);
    assertThatThrownBy(() -> admin.lock("ops", s.userId(), "x".repeat(501))).isInstanceOf(DomainException.class);
  }

  @Test
  void resetForcesPasswordChangeAndRevokesEverySession() {
    String temp = admin.resetPassword("ops", s.userId(), "user lost access");
    assertThat(temp).hasSize(16).matches("[A-Za-z0-9]+");
    assertThat(f.accounts.get(s.userId()).mustChangePassword()).isTrue();
    assertThat(f.accounts.get(s.userId()).passwordHash()).isEqualTo("h:" + temp);
    assertThat(f.sessionStore.live(s.userId())).isEmpty();
    assertThat(f.revocations(s.userId())).containsExactlyInAnyOrder("device:" + s.deviceId(), "device:" + d2);
    assertThat(f.adminActions).containsExactly("ops reset_password " + s.userId() + " user lost access");
  }

  @Test
  void lockRevokesDevicesNotThePrincipal() {
    admin.lock("ops", s.userId(), "spam");
    assertThat(f.accounts.get(s.userId()).status()).isEqualTo(AccountStatus.LOCKED);
    assertThat(f.revocations(s.userId())).containsExactlyInAnyOrder("device:" + s.deviceId(), "device:" + d2);
    assertThat(f.events(s.userId())).containsExactly(AccountState.Status.STATUS_ACTIVE,
        AccountState.Status.STATUS_LOCKED);
    admin.unlock("ops", s.userId(), "appeal accepted");
    assertThat(f.accounts.get(s.userId()).status()).isEqualTo(AccountStatus.ACTIVE);
    assertThat(f.events(s.userId())).endsWith(AccountState.Status.STATUS_ACTIVE);
  }

  @Test
  void deletedAccountCannotBeLocked() {
    admin.delete("ops", s.userId(), "asked");
    assertThatThrownBy(() -> admin.lock("ops", s.userId(), "x")).isInstanceOfSatisfying(DomainException.class,
        e -> assertThat(e.code()).isEqualTo("not_found"));
  }
  /** Final review minor: the kind asked is the kind searched; a phone may come with spaces or without its +. */
  @Test
  void findUsesTheKindAsked() {
    f.accountStore.setIdentifier(s.userId(), com.pangate.account.domain.LoginKind.PHONE, "+84901234567");
    assertThatThrownBy(() -> admin.find(com.pangate.account.domain.LoginKind.EMAIL, "lannguyen"))
        .isInstanceOf(DomainException.class);
    assertThatThrownBy(() -> admin.find(com.pangate.account.domain.LoginKind.USERNAME, "l@x.vn"))
        .isInstanceOf(DomainException.class);
    assertThat(admin.find(com.pangate.account.domain.LoginKind.PHONE, " 84901234567").account().userId())
        .isEqualTo(s.userId());
    assertThat(admin.find(com.pangate.account.domain.LoginKind.EMAIL, " L@x.vn").account().userId())
        .isEqualTo(s.userId());
  }

  /** Final review minor: the admin_actions row of a deletion is written in the deletion's transaction. */
  @Test
  void deleteIsLoggedInsideItsTransaction() {
    admin.delete("ops", s.userId(), "asked");
    assertThat(f.adminActionsInTx).containsExactly(true);
  }
}
