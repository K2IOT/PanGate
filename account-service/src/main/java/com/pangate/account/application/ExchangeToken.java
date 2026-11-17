package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.application.port.AuthApi;
import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.config.AccountProperties;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.RateLimitedException;
import com.pangate.account.domain.Session;
import com.pangate.account.infrastructure.crypto.RefreshTokens;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * {@code POST /v1/sessions/token} (account spec §2, §4.2): a live, recently used session of a live account gets a
 * 10-minute connect token for its device from auth-service. {@code last_used_at} is written at most every
 * {@code writeEvery}; at most {@code tokensPerSessionPerHour} exchanges per session (spec §7).
 */
public final class ExchangeToken {

  private final SessionStore sessions;
  private final AccountStore accounts;
  private final AuthApi auth;
  private final AttemptCounter counter;
  private final AccountProperties.Limits limits;
  private final Clock clock;
  private final Duration idleTtl;
  private final Duration writeEvery;
  private final Metrics metrics;

  public ExchangeToken(SessionStore sessions, AccountStore accounts, AuthApi auth, AttemptCounter counter,
      AccountProperties.Limits limits, Clock clock, Duration idleTtl, Duration writeEvery, Metrics metrics) {
    this.sessions = sessions;
    this.accounts = accounts;
    this.auth = auth;
    this.counter = counter;
    this.limits = limits;
    this.clock = clock;
    this.idleTtl = idleTtl;
    this.writeEvery = writeEvery;
    this.metrics = metrics;
  }

  private DomainException invalid() {
    metrics.count("account.tokens", "result", "invalid_session");
    return new DomainException("invalid_session", "log in again");
  }

  public AuthApi.ConnectToken exchange(String refreshToken) {
    if (!RefreshTokens.looksValid(refreshToken)) {
      throw invalid();
    }
    Instant now = clock.now();
    Session s = sessions.byRefreshHash(RefreshTokens.hash(refreshToken)).orElseThrow(this::invalid);
    Optional<Account> a = accounts.byId(s.userId()).filter(x -> x.status() != AccountStatus.DELETED);
    if (a.isEmpty()) {
      throw invalid();
    }
    // the account's status first: a lock revoked every session, and the device should hear "locked", not "log in"
    if (a.get().status() == AccountStatus.LOCKED) {
      metrics.count("account.tokens", "result", "locked");
      throw new DomainException("account_locked", "this account is locked");
    }
    if (s.revokedAt() != null || s.lastUsedAt().isBefore(now.minus(idleTtl))) {
      throw invalid();
    }
    if (counter.increment("tok:" + s.deviceId(), Duration.ofHours(1)) > limits.tokensPerSessionPerHour()) {
      metrics.count("account.ratelimited", "what", "token");
      throw new RateLimitedException("rate_limited", 60);
    }
    AuthApi.ConnectToken t;
    try {
      t = auth.mint(s.userId(), s.deviceId());
    } catch (AuthApi.AuthUnavailableException | AuthApi.AuthRejectedException e) {
      metrics.count("account.tokens", "result", "unavailable");
      throw new DomainException("unavailable", "try again shortly");
    }
    if (s.lastUsedAt().isBefore(now.minus(writeEvery))) {
      sessions.touch(s.deviceId(), now);
    }
    metrics.count("account.tokens", "result", "ok");
    return t;
  }
}
