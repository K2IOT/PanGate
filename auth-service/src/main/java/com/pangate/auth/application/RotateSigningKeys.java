package com.pangate.auth.application;

import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.SigningKeyStore;
import com.pangate.auth.config.AuthProperties;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import com.pangate.auth.infrastructure.crypto.Es256TokenSigner;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * One pass of the key life cycle (spec §3). The caller holds the {@code auth.rotate-keys} advisory lock and a
 * transaction; the unique index {@code one_active_key} is the second line of defence.
 */
public final class RotateSigningKeys {

  static final Duration RENEW_BEFORE_EXPIRY = Duration.ofDays(1);

  private final SigningKeyStore store;
  private final Clock clock;
  private final AuthProperties.Keys cfg;

  public RotateSigningKeys(SigningKeyStore store, Clock clock, AuthProperties.Keys cfg) {
    this.store = store;
    this.clock = clock;
    this.cfg = cfg;
  }

  /** @return true if anything changed */
  public boolean runOnce() {
    Instant now = clock.now();
    List<SigningKey> keys = store.all();
    boolean changed = false;
    Optional<SigningKey> active = first(keys, KeyState.ACTIVE);
    Optional<SigningKey> pending = first(keys, KeyState.PENDING);

    if (pending.isPresent() && !now.isBefore(pending.get().activateAt())) {
      store.retireActive(now, now.plus(cfg.overlap()));
      store.update(pending.get().withState(KeyState.ACTIVE, null, null));
      active = pending;
      pending = Optional.empty();
      changed = true;
    } else if (active.isEmpty()) {
      // First run, or the active key cannot be decrypted: a signing key is needed now (spec §3 rule 1, §6).
      store.retireActive(now, now.plus(cfg.overlap()));
      SigningKey k = Es256TokenSigner.newKey(now, KeyState.ACTIVE);
      store.insert(k);
      active = Optional.of(k);
      changed = true;
    }

    Instant renewAt = active.get().activateAt().plus(cfg.lifetime()).minus(RENEW_BEFORE_EXPIRY);
    if (pending.isEmpty() && !now.isBefore(renewAt)) {
      store.insert(Es256TokenSigner.newKey(now.plus(cfg.publishAhead()), KeyState.PENDING));
      changed = true;
    }

    for (SigningKey k : keys) {
      if (k.state() == KeyState.RETIRED && k.removeAt() != null && !now.isBefore(k.removeAt())) {
        store.delete(k.kid());
        changed = true;
      }
    }
    return changed;
  }

  /** Publishes a new pending key now (key compromise, Admin {@code /keys:rotate-now}); false if one exists. */
  public boolean forcePending() {
    if (first(store.all(), KeyState.PENDING).isPresent()) {
      return false;
    }
    store.insert(Es256TokenSigner.newKey(clock.now().plus(cfg.publishAhead()), KeyState.PENDING));
    return true;
  }

  private static Optional<SigningKey> first(List<SigningKey> keys, KeyState s) {
    return keys.stream().filter(k -> k.state() == s).findFirst();
  }
}
