package com.pangate.auth.application;

import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.KeyFactory;
import com.pangate.auth.application.port.SigningKeyStore;
import com.pangate.auth.domain.KeyPolicy;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * One pass of the key life cycle (spec §3). The caller holds the {@code auth.rotate-keys} advisory lock and a
 * transaction; the unique index {@code one_active_key} is the second line of defence.
 */
public final class RotateSigningKeys {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RotateSigningKeys.class);

  static final Duration RENEW_BEFORE_EXPIRY = Duration.ofDays(1);

  private final SigningKeyStore store;
  private final Clock clock;
  private final KeyPolicy cfg;
  private final KeyFactory keyFactory;

  public RotateSigningKeys(SigningKeyStore store, Clock clock, KeyPolicy cfg, KeyFactory keys) {
    this.store = store;
    this.clock = clock;
    this.cfg = cfg;
    this.keyFactory = keys;
  }

  /** @return true if anything changed */
  public boolean runOnce() {
    Instant now = clock.now();
    SigningKeyStore.Snapshot snapshot = store.load();
    List<SigningKey> keys = snapshot.usable();
    boolean changed = false;
    Optional<SigningKey> active = first(keys, KeyState.ACTIVE);
    Optional<SigningKey> pending = first(keys, KeyState.PENDING);
    // a pending key this pod cannot decrypt still counts as published-ahead: never publish a second one
    boolean pendingPublished = snapshot.published().stream().anyMatch(p -> p.state() == KeyState.PENDING);

    if (pending.isPresent() && !now.isBefore(pending.get().activateAt())) {
      store.retireActive(now, now.plus(cfg.overlap()));
      store.update(pending.get().withState(KeyState.ACTIVE, null, null));
      active = pending;
      pending = Optional.empty();
      pendingPublished = false;
      changed = true;
    } else if (active.isEmpty()) {
      Optional<SigningKeyStore.PublishedKey> unreadable = snapshot.published().stream()
          .filter(p -> p.state() == KeyState.ACTIVE).findFirst();
      if (unreadable.isPresent() && !kekProvenHere(snapshot, unreadable.get())) {
        // This pod lacks the KEK (or holds wrong bytes): other pods may sign with this key. Do not replace it.
        log.error("active signing key {} cannot be decrypted with KEK {} on this pod; leaving it to pods that can",
            unreadable.get().kid(), unreadable.get().kekId());
        return false;
      }
      // First run, or the active key is corrupt although its KEK opens other keys: a signing key is needed now.
      store.retireActive(now, now.plus(cfg.overlap()));
      SigningKey k = keyFactory.newKey(now, KeyState.ACTIVE);
      store.insert(k);
      active = Optional.of(k);
      changed = true;
    }

    Instant renewAt = active.get().activateAt().plus(cfg.lifetime()).minus(RENEW_BEFORE_EXPIRY);
    if (!pendingPublished && !now.isBefore(renewAt)) {
      store.insert(keyFactory.newKey(now.plus(cfg.publishAhead()), KeyState.PENDING));
      changed = true;
    }

    for (SigningKeyStore.PublishedKey k : snapshot.published()) {  // decryptable or not
      if (k.state() == KeyState.RETIRED && k.removeAt() != null && !now.isBefore(k.removeAt())) {
        store.delete(k.kid());
        changed = true;
      }
    }
    return changed;
  }

  /** The KEK of {@code k} demonstrably works here: another row sealed with the same KEK decrypts on this pod. */
  private static boolean kekProvenHere(SigningKeyStore.Snapshot s, SigningKeyStore.PublishedKey k) {
    return s.published().stream().anyMatch(p -> p.decryptable() && !p.kid().equals(k.kid())
        && p.kekId().equals(k.kekId()));
  }

  /** Publishes a new pending key now (key compromise, Admin {@code /keys:rotate-now}); false if one exists. */
  public boolean forcePending() {
    if (first(store.all(), KeyState.PENDING).isPresent()) {
      return false;
    }
    store.insert(keyFactory.newKey(clock.now().plus(cfg.publishAhead()), KeyState.PENDING));
    return true;
  }

  private static Optional<SigningKey> first(List<SigningKey> keys, KeyState s) {
    return keys.stream().filter(k -> k.state() == s).findFirst();
  }
}
