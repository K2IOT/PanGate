package com.pangate.account.application.port;

import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.Login;
import com.pangate.account.domain.LoginKind;
import com.pangate.account.domain.Username;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** accounts + identifiers (account spec §3). Writes run inside {@link Transactions}. */
public interface AccountStore {

  /** Throws {@link TakenException} naming the unique field already used. */
  void insert(Account a);

  Optional<Account> byId(String userId);

  Optional<Account> byLogin(Login login);

  Optional<Account> byUsernameKey(String key);

  List<Account> byIds(Collection<String> userIds);

  /**
   * Writes only what is given, against the current row (so concurrent patches of different fields both stick): a
   * null {@code displayName} leaves it; {@code setBio} false leaves the bio, true sets it (null clears it).
   */
  void updateProfile(String userId, String displayName, String bio, boolean setBio);

  /** Throws {@link TakenException} ({@code username_taken}). */
  void updateUsername(String userId, Username username);

  /**
   * {@code value} null removes it; throws {@link TakenException}. Under the account's row lock, refuses
   * ({@code invalid_request}) a removal that would leave neither an email nor a phone number, so two racing removals
   * cannot both succeed.
   */
  void setIdentifier(String userId, LoginKind kind, String value);

  void setPassword(String userId, String hash, boolean mustChange);

  void setStatus(String userId, AccountStatus status);

  /**
   * account spec §5.3: personal fields to NULL, identifiers deleted, status deleted; the row stays. Row-locks the
   * account first; already deleted (or unknown) is {@code not_found}.
   */
  void erase(String userId);
}
