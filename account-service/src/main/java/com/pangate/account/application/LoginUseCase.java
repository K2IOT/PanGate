package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.PasswordHasher;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Sleeper;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.config.AccountProperties;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.Email;
import com.pangate.account.domain.Login;
import com.pangate.account.domain.PasswordPolicy;
import com.pangate.account.domain.Phone;
import com.pangate.account.domain.RateLimitedException;
import com.pangate.account.domain.Username;
import com.pangate.account.infrastructure.crypto.HashGate;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * {@code POST /v1/sessions} (account spec §4.2, §7). A wrong login and a wrong password are one error and take the
 * same time. Failures count per login + IP (10 → blocked 15 min), per IP (50 → blocked) and per login from every IP
 * (100 → each further try waits 2 s, never a block: usernames are public). The password is checked before the account
 * status, so a guesser learns nothing about a lock.
 */
public final class LoginUseCase {

  private final AccountStore accounts;
  private final SessionStore sessions;
  private final PasswordHasher hasher;
  private final HashGate gate;
  private final AttemptCounter counter;
  private final AccountProperties.Limits limits;
  private final Sleeper sleeper;
  private final Transactions tx;
  private final Metrics metrics;

  public LoginUseCase(AccountStore accounts, SessionStore sessions, PasswordHasher hasher, HashGate gate,
      AttemptCounter counter, AccountProperties.Limits limits, Sleeper sleeper, Transactions tx, Metrics metrics) {
    this.accounts = accounts;
    this.sessions = sessions;
    this.hasher = hasher;
    this.gate = gate;
    this.counter = counter;
    this.limits = limits;
    this.sleeper = sleeper;
    this.tx = tx;
    this.metrics = metrics;
  }

  static String pairKey(Login l, String ip) {
    return "login:" + l.key() + "|" + ip;
  }

  public NewSession login(String loginText, char[] password, char[] newPassword, DeviceInfo device, String ip) {
    Login l = Login.parse(loginText);
    Account a = verify(l, password, ip);
    if (a.status() == AccountStatus.LOCKED) {
      metrics.count("account.logins", "result", "locked");
      throw new DomainException("account_locked", "this account is locked");
    }
    if (a.mustChangePassword()) {
      if (newPassword == null) {
        metrics.count("account.logins", "result", "password_change_required");
        throw new DomainException("password_change_required", "set a new password to log in");
      }
      char[] next = PasswordPolicy.check(newPassword, new Username(a.username(), a.username().toLowerCase()),
          a.email() == null ? null : new Email(a.email()), a.phone() == null ? null : new Phone(a.phone()));
      String hash = gate.run(() -> hasher.hash(next));
      accounts.setPassword(a.userId(), hash, false);
    }
    NewSession s = tx.inTx(() -> Sessions.open(sessions, a.userId(), device, Instant.now()));
    metrics.count("account.logins", "result", "ok");
    return s;
  }

  /**
   * The account when {@code password} is its password, after the attempt limits; a wrong password is counted and
   * answered {@code invalid_credentials}, as is a login that names no live account. Also used by password changes.
   */
  public Account verify(Login l, char[] password, String ip) {
    String pair = pairKey(l, ip);
    String ipKey = "ip:" + ip;
    Optional<Duration> blocked = counter.blockedFor(pair).or(() -> counter.blockedFor(ipKey));
    if (blocked.isPresent()) {
      metrics.count("account.ratelimited", "what", "login");
      throw new RateLimitedException("too_many_attempts", blocked.get().toSeconds());
    }
    if (counter.count("loginall:" + l.key()) >= limits.loginAllFailures()) {
      sleeper.sleep(limits.loginAllDelay());
    }
    char[] pw = PasswordPolicy.normalise(password == null ? new char[0] : password);
    Optional<Account> found = accounts.byLogin(l).filter(a -> a.status() != AccountStatus.DELETED);
    boolean ok = gate.run(() -> {
      if (found.isEmpty()) {
        hasher.burn();
        return false;
      }
      return hasher.matches(pw, found.get().passwordHash());
    });
    if (!ok) {
      if (counter.increment(pair, limits.failureWindow()) >= limits.loginIpFailures()) {
        counter.block(pair, limits.block());
      }
      if (counter.increment(ipKey, limits.failureWindow()) >= limits.ipFailures()) {
        counter.block(ipKey, limits.block());
      }
      counter.increment("loginall:" + l.key(), limits.failureWindow());
      metrics.count("account.logins", "result", "invalid_credentials");
      throw new DomainException("invalid_credentials", "wrong login or password");
    }
    counter.clear(pair);
    return found.get();
  }
}
