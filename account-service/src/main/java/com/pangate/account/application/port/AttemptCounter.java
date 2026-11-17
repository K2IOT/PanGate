package com.pangate.account.application.port;

import java.time.Duration;
import java.util.Optional;

/**
 * Counters and blocks for the limits of account spec §7, shared between pods (Redis), with a per-pod fallback.
 * Windows are fixed: the first increment of a key starts its window.
 */
public interface AttemptCounter {

  /** Adds one; the count within the key's current window. */
  long increment(String key, Duration window);

  long count(String key);

  /** Takes back one increment of the key's current window (never below zero); nothing when the window is over. */
  void decrement(String key);

  void clear(String key);

  void block(String key, Duration duration);

  /** Time left on a block, empty when not blocked. */
  Optional<Duration> blockedFor(String key);
}
