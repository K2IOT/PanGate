package com.pangate.account.application;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.config.AccountProperties;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.ProfileRules;
import com.pangate.account.domain.RateLimitedException;
import com.pangate.account.domain.Username;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Profiles (account spec §4.2): mine, and the public profile of others — exact username, by id, or up to 100 ids. A
 * deleted account shows only {@code deleted}; a locked one shows its profile. Lookups of others are limited per user
 * (spec §7).
 */
public final class Profiles {

  public static final int MAX_BATCH = 100;

  public record PublicProfile(String userId, String username, String displayName, String bio, boolean deleted) {}

  public record MeView(String userId, String username, String displayName, String bio, String email, String phone,
      Instant createdAt) {}

  private final AccountStore accounts;
  private final Set<String> reserved;
  private final AttemptCounter counter;
  private final AccountProperties.Limits limits;
  private final com.pangate.account.application.port.Outbox outbox;
  private final com.pangate.account.application.port.Transactions tx;

  public Profiles(AccountStore accounts, Set<String> reserved, AttemptCounter counter,
      AccountProperties.Limits limits, com.pangate.account.application.port.Outbox outbox,
      com.pangate.account.application.port.Transactions tx) {
    this.accounts = accounts;
    this.reserved = reserved;
    this.counter = counter;
    this.limits = limits;
    this.outbox = outbox;
    this.tx = tx;
  }

  static PublicProfile publicOf(Account a) {
    return a.status() == AccountStatus.DELETED ? new PublicProfile(a.userId(), null, null, null, true)
        : new PublicProfile(a.userId(), a.username(), a.displayName(), a.bio(), false);
  }

  private Account mine(Caller c) {
    return accounts.byId(c.userId()).filter(a -> a.status() != AccountStatus.DELETED)
        .orElseThrow(() -> new DomainException("not_found", "no such account"));
  }

  private void lookup(Caller c) {
    if (counter.increment("look:" + c.userId(), Duration.ofMinutes(1)) > limits.lookupsPerUserPerMinute()) {
      throw new RateLimitedException("rate_limited", 60);
    }
  }

  public MeView me(Caller c) {
    Account a = mine(c);
    return new MeView(a.userId(), a.username(), a.displayName(), a.bio(), a.email(), a.phone(), a.createdAt());
  }

  /** Null leaves a field as it is; an empty bio clears it. Only the fields given are written. */
  public void update(Caller c, String displayName, String bio) {
    Account a = mine(c);
    String name = displayName == null ? null : ProfileRules.displayName(displayName);
    String b = bio == null || bio.isBlank() ? null : ProfileRules.bio(bio);
    if (name == null) {
      accounts.updateProfile(a.userId(), null, b, bio != null);
      return;
    }
    // offline notifications spec §2.2: account.users carries the new name, written with it in one transaction
    tx.inTx(() -> {
      accounts.updateProfile(a.userId(), name, b, bio != null);
      // CH4 deferred minor: the account's own status (a locked account renamed is still locked)
      outbox.event(a.userId(), a.status() == com.pangate.account.domain.AccountStatus.LOCKED
          ? com.pangate.contracts.v1.AccountState.Status.STATUS_LOCKED
          : com.pangate.contracts.v1.AccountState.Status.STATUS_ACTIVE);
    });
  }

  /** The old username is free at once (spec §4.2). */
  public void changeUsername(Caller c, String username) {
    Account a = mine(c);
    accounts.updateUsername(a.userId(), Username.of(username, reserved));
  }

  public PublicProfile byId(Caller c, String userId) {
    lookup(c);
    return accounts.byId(userId).map(Profiles::publicOf)
        .orElseThrow(() -> new DomainException("not_found", "no such user"));
  }

  /** Exact username, any case; a deleted account has no username, so it is never found. */
  public Optional<PublicProfile> byUsername(Caller c, String username) {
    lookup(c);
    if (username == null || username.isBlank()) {
      return Optional.empty();
    }
    return accounts.byUsernameKey(username.trim().toLowerCase(java.util.Locale.ROOT)).map(Profiles::publicOf);
  }

  /** In the order asked; unknown ids are left out. */
  public List<PublicProfile> batch(Caller c, List<String> userIds) {
    if (userIds.size() > MAX_BATCH) {
      throw new DomainException("invalid_request", "at most " + MAX_BATCH + " userIds");
    }
    lookup(c);
    Map<String, Account> found = accounts.byIds(userIds).stream()
        .collect(Collectors.toMap(Account::userId, Function.identity(), (x, y) -> x));
    return userIds.stream().distinct().filter(found::containsKey).map(id -> publicOf(found.get(id))).toList();
  }
}
