package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AdminLog;
import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.PasswordHasher;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.Login;
import com.pangate.account.domain.LoginKind;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.contracts.v1.AccountState;
import java.security.SecureRandom;
import java.util.List;

/**
 * The admin API (account spec §5): look up, lock / unlock, reset the password, delete. Every change needs a reason
 * (≤ 500 characters) and is logged in admin_actions. Locks and resets revoke each device, never the principal, so
 * an unlock or a new login works at once (ADR 0036).
 */
public final class AdminAccounts {

  public record AdminView(Account account, int liveSessions, List<AdminLog.Entry> actions) {}

  private static final String TEMP_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789";
  private static final SecureRandom RANDOM = new SecureRandom();

  private final AccountStore accounts;
  private final SessionStore sessions;
  private final Outbox outbox;
  private final AdminLog log;
  private final PasswordHasher hasher;
  private final HashGate gate;
  private final DeleteAccount deleter;
  private final Transactions tx;
  private final Clock clock;
  private final Metrics metrics;

  public AdminAccounts(AccountStore accounts, SessionStore sessions, Outbox outbox, AdminLog log,
      PasswordHasher hasher, HashGate gate, DeleteAccount deleter, Transactions tx, Clock clock, Metrics metrics) {
    this.accounts = accounts;
    this.sessions = sessions;
    this.outbox = outbox;
    this.log = log;
    this.hasher = hasher;
    this.gate = gate;
    this.deleter = deleter;
    this.tx = tx;
    this.clock = clock;
    this.metrics = metrics;
  }

  static String reason(String reason) {
    if (reason == null || reason.isBlank() || reason.length() > 500) {
      throw new DomainException("invalid_request", "a reason of 1..500 characters is required");
    }
    return reason.trim();
  }

  private Account live(String userId) {
    return accounts.byId(userId).filter(a -> a.status() != AccountStatus.DELETED)
        .orElseThrow(() -> new DomainException("not_found", "no such account"));
  }

  private AdminView view(Account a) {
    return new AdminView(a, sessions.live(a.userId()).size(), log.of(a.userId()));
  }

  private void event(String userId, AccountState.Status s) {
    outbox.event(userId, s);
  }

  public AdminView get(String userId) {
    return view(accounts.byId(userId).orElseThrow(() -> new DomainException("not_found", "no such account")));
  }

  /** The kind asked is the kind searched; a phone may come with spaces, or without its + (a URL turns + into a space). */
  public AdminView find(LoginKind kind, String value) {
    String v = value == null ? "" : value.trim();
    String key = switch (kind) {
      case PHONE -> {
        String digits = v.replaceAll("\\s+", "");
        yield digits.startsWith("+") ? digits : "+" + digits;
      }
      case EMAIL, USERNAME -> v.toLowerCase(java.util.Locale.ROOT);
    };
    return view(accounts.byLogin(new Login(kind, key))
        .orElseThrow(() -> new DomainException("not_found", "no such account")));
  }

  public void lock(String actor, String userId, String reason) {
    String r = reason(reason);
    Account a = live(userId);
    tx.inTx(() -> {
      accounts.setStatus(a.userId(), AccountStatus.LOCKED);
      ManageSessions.revokeAll(sessions, outbox, a.userId(), null);
      event(a.userId(), AccountState.Status.STATUS_LOCKED);
      log.record(actor, "lock", a.userId(), r);
    });
    metrics.count("account.admin.actions", "action", "lock");
  }

  public void unlock(String actor, String userId, String reason) {
    String r = reason(reason);
    Account a = live(userId);
    tx.inTx(() -> {
      accounts.setStatus(a.userId(), AccountStatus.ACTIVE);
      event(a.userId(), AccountState.Status.STATUS_ACTIVE);
      log.record(actor, "unlock", a.userId(), r);
    });
    metrics.count("account.admin.actions", "action", "unlock");
  }

  /** The temporary password, shown once; the next login must set a new one (spec §5.2). */
  public String resetPassword(String actor, String userId, String reason) {
    String r = reason(reason);
    Account a = live(userId);
    StringBuilder b = new StringBuilder(16);
    for (int i = 0; i < 16; i++) {
      b.append(TEMP_CHARS.charAt(RANDOM.nextInt(TEMP_CHARS.length())));
    }
    String temp = b.toString();
    String hash = gate.run(() -> hasher.hash(temp.toCharArray()));
    tx.inTx(() -> {
      accounts.setPassword(a.userId(), hash, true);
      ManageSessions.revokeAll(sessions, outbox, a.userId(), null);
      log.record(actor, "reset_password", a.userId(), r);
    });
    metrics.count("account.admin.actions", "action", "reset_password");
    return temp;
  }

  public void delete(String actor, String userId, String reason) {
    String r = reason(reason);
    Account a = live(userId);
    tx.inTx(() -> {
      deleter.deleteWithin(a.userId());
      log.record(actor, "delete", a.userId(), r);
    });
    metrics.count("account.admin.actions", "action", "delete");
  }
}
