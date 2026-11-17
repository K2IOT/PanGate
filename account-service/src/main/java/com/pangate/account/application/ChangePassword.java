package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.PasswordHasher;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.Email;
import com.pangate.account.domain.Login;
import com.pangate.account.domain.LoginKind;
import com.pangate.account.domain.PasswordPolicy;
import com.pangate.account.domain.Phone;
import com.pangate.account.domain.Username;
import com.pangate.account.infrastructure.crypto.HashGate;
import java.util.Locale;

/**
 * {@code POST /v1/me/password} (account spec §4.2): the current password is checked like a login (a wrong one counts
 * toward the login limits); every other device is logged out with a {@code device} revocation each, the calling
 * device keeps its session (ADR 0036).
 */
public final class ChangePassword {

  private final AccountStore accounts;
  private final SessionStore sessions;
  private final Outbox outbox;
  private final PasswordHasher hasher;
  private final HashGate gate;
  private final LoginUseCase login;
  private final Transactions tx;

  public ChangePassword(AccountStore accounts, SessionStore sessions, Outbox outbox, PasswordHasher hasher,
      HashGate gate, LoginUseCase login, Transactions tx) {
    this.accounts = accounts;
    this.sessions = sessions;
    this.outbox = outbox;
    this.hasher = hasher;
    this.gate = gate;
    this.login = login;
    this.tx = tx;
  }

  static Account live(AccountStore accounts, Caller c) {
    return accounts.byId(c.userId()).filter(a -> a.status() != AccountStatus.DELETED)
        .orElseThrow(() -> new DomainException("not_found", "no such account"));
  }

  static Login loginOf(Account a) {
    return new Login(LoginKind.USERNAME, a.username().toLowerCase(Locale.ROOT));
  }

  public void change(Caller c, char[] current, char[] next) {
    Account a = live(accounts, c);
    login.verify(loginOf(a), current, c.ip());
    char[] pw = PasswordPolicy.check(next, new Username(a.username(), a.username().toLowerCase(Locale.ROOT)),
        a.email() == null ? null : new Email(a.email()), a.phone() == null ? null : new Phone(a.phone()));
    String hash = gate.run(() -> hasher.hash(pw));
    tx.inTx(() -> {
      accounts.setPassword(a.userId(), hash, false);
      ManageSessions.revokeAll(sessions, outbox, a.userId(), c.deviceId());
    });
  }
}
