package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.Email;
import com.pangate.account.domain.LoginKind;
import com.pangate.account.domain.Phone;

/**
 * {@code PUT /v1/me/email}, {@code PUT /v1/me/phone} (account spec §4.2): needs the password; null removes it, but an
 * account keeps at least one of the two. New values are unverified, like every identifier in CH2.
 */
public final class ChangeIdentifier {

  private final AccountStore accounts;
  private final LoginUseCase login;
  private final Transactions tx;

  public ChangeIdentifier(AccountStore accounts, LoginUseCase login, Transactions tx) {
    this.accounts = accounts;
    this.login = login;
    this.tx = tx;
  }

  public void set(Caller c, LoginKind kind, String value, char[] password) {
    Account a = ChangePassword.live(accounts, c);
    login.verify(ChangePassword.loginOf(a), password, c.ip());
    String v = value == null || value.isBlank() ? null
        : kind == LoginKind.EMAIL ? Email.of(value).value() : Phone.of(value).value();
    String other = kind == LoginKind.EMAIL ? a.phone() : a.email();
    if (v == null && other == null) {
      throw new DomainException("invalid_request", "an account keeps an email or a phone number");
    }
    tx.inTx(() -> accounts.setIdentifier(a.userId(), kind, v));
  }
}
