package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Account;
import com.pangate.contracts.v1.AccountState;

/**
 * Account deletion (account spec §5.3), by the user ({@code DELETE /v1/me}, password needed) or an admin. One
 * transaction: every session revoked with a device revocation each and a {@code principal} one (the user never logs
 * in again, ADR 0036), personal data erased, {@code AccountState DELETED}. The userId stays and is never reused.
 */
public final class DeleteAccount {

  private final AccountStore accounts;
  private final SessionStore sessions;
  private final Outbox outbox;
  private final LoginUseCase login;
  private final Transactions tx;
  private final Clock clock;

  /** {@code login} checks the password of a self-deletion; null where only admins delete (tests). */
  public DeleteAccount(AccountStore accounts, SessionStore sessions, Outbox outbox, LoginUseCase login,
      Transactions tx, Clock clock) {
    this.accounts = accounts;
    this.sessions = sessions;
    this.outbox = outbox;
    this.login = login;
    this.tx = tx;
    this.clock = clock;
  }

  public void deleteSelf(Caller c, char[] password) {
    Account a = ChangePassword.live(accounts, c);
    login.verify(ChangePassword.loginOf(a), password, c.ip());
    delete(a.userId());
  }

  public void delete(String userId) {
    tx.inTx(() -> deleteWithin(userId));
  }

  /** The deletion's steps, inside a transaction the caller holds (the admin API adds its log row to it). */
  void deleteWithin(String userId) {
    ManageSessions.revokeAll(sessions, outbox, userId, null);
    outbox.revokePrincipal(userId);
    accounts.erase(userId);
    outbox.event(userId, AccountState.Status.STATUS_DELETED);
  }
}
