package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.PasswordHasher;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.TakenException;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.config.AccountProperties;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.Email;
import com.pangate.account.domain.Ids;
import com.pangate.account.domain.PasswordPolicy;
import com.pangate.account.domain.Phone;
import com.pangate.account.domain.ProfileRules;
import com.pangate.account.domain.RateLimitedException;
import com.pangate.account.domain.Username;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.contracts.v1.AccountState;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/**
 * {@code POST /v1/accounts} (account spec §4.2): account, identifiers, first session and {@code AccountState ACTIVE}
 * in one transaction. At most {@code registrationsPerIpPerHour} per IP (spec §7).
 */
public final class Register {

  private final AccountStore accounts;
  private final SessionStore sessions;
  private final Outbox outbox;
  private final PasswordHasher hasher;
  private final HashGate gate;
  private final AttemptCounter counter;
  private final AccountProperties.Limits limits;
  private final Set<String> reserved;
  private final com.pangate.account.application.port.Transactions tx;
  private final Metrics metrics;

  public Register(AccountStore accounts, SessionStore sessions, Outbox outbox, PasswordHasher hasher, HashGate gate,
      AttemptCounter counter, AccountProperties.Limits limits, Set<String> reserved, Transactions tx,
      Metrics metrics) {
    this.accounts = accounts;
    this.sessions = sessions;
    this.outbox = outbox;
    this.hasher = hasher;
    this.gate = gate;
    this.counter = counter;
    this.limits = limits;
    this.reserved = reserved;
    this.tx = tx;
    this.metrics = metrics;
  }

  public NewSession register(String username, char[] password, String email, String phone, String displayName,
      DeviceInfo device, String ip) {
    if (counter.count("reg:" + ip) >= limits.registrationsPerIpPerHour()) {
      metrics.count("account.ratelimited", "what", "register");
      throw new RateLimitedException("rate_limited", 3600);
    }
    if (isBlank(email) && isBlank(phone)) {
      throw new DomainException("invalid_request", "an email or a phone number is required");
    }
    Username u = Username.of(username, reserved);
    Email e = isBlank(email) ? null : Email.of(email);
    Phone p = isBlank(phone) ? null : Phone.of(phone);
    String name = displayName == null ? u.display() : ProfileRules.displayName(displayName);
    char[] pw = PasswordPolicy.check(password, u, e, p);
    String hash = gate.run(() -> hasher.hash(pw));
    String userId = Ids.next();
    Instant now = Instant.now();
    try {
      NewSession s = tx.inTx(() -> {
        accounts.insert(new Account(userId, u.display(), name, null, hash, false, AccountStatus.ACTIVE,
            e == null ? null : e.value(), p == null ? null : p.value(), now, null));
        outbox.event(userId, AccountState.newBuilder().setUserId(userId).setStatus(AccountState.Status.STATUS_ACTIVE)
            .setUpdatedAtMs(now.toEpochMilli()).build());
        return Sessions.open(sessions, userId, device, now);
      });
      counter.increment("reg:" + ip, Duration.ofHours(1));   // only accounts actually created count (spec §7)
      metrics.count("account.registrations", "result", "ok");
      return s;
    } catch (TakenException ex) {
      metrics.count("account.registrations", "result", ex.code());
      throw ex;
    }
  }

  static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }
}
