package com.pangate.account.infrastructure.redis;

import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.application.port.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Per-pod counters: the fallback when Redis cannot be used (account spec §7), and the counter of unit tests. */
public final class InMemoryAttemptCounter implements AttemptCounter {

  private record Window(long count, Instant end) {}

  private final Clock clock;
  private final Map<String, Window> counts = new ConcurrentHashMap<>();
  private final Map<String, Instant> blocks = new ConcurrentHashMap<>();
  /** Expired entries are dropped every this many writes, so a long Redis outage cannot fill the heap. */
  static final int PURGE_EVERY = 1024;
  private final java.util.concurrent.atomic.AtomicLong writes = new java.util.concurrent.atomic.AtomicLong();

  private void maybePurge() {
    if (writes.incrementAndGet() % PURGE_EVERY == 0) {
      purge();
    }
  }

  public InMemoryAttemptCounter(Clock clock) {
    this.clock = clock;
  }

  @Override
  public long increment(String key, Duration window) {
    maybePurge();
    Instant now = clock.now();
    return counts.compute(key, (k, w) -> w == null || !now.isBefore(w.end()) ? new Window(1, now.plus(window))
        : new Window(w.count() + 1, w.end())).count();
  }

  @Override
  public long count(String key) {
    Window w = counts.get(key);
    return w == null || !clock.now().isBefore(w.end()) ? 0 : w.count();
  }

  @Override
  public void decrement(String key) {
    Instant now = clock.now();
    counts.computeIfPresent(key, (k, w) -> !now.isBefore(w.end()) ? null
        : new Window(Math.max(0, w.count() - 1), w.end()));
  }

  @Override
  public void clear(String key) {
    counts.remove(key);
  }

  @Override
  public void block(String key, Duration duration) {
    maybePurge();
    blocks.put(key, clock.now().plus(duration));
  }

  @Override
  public Optional<Duration> blockedFor(String key) {
    Instant until = blocks.get(key);
    Instant now = clock.now();
    if (until == null || !now.isBefore(until)) {
      return Optional.empty();
    }
    return Optional.of(Duration.between(now, until));
  }

  /** Entries held (counts + blocks), for tests. */
  int size() {
    return counts.size() + blocks.size();
  }

  /** Drops expired entries. */
  public void purge() {
    Instant now = clock.now();
    counts.values().removeIf(w -> !now.isBefore(w.end()));
    blocks.values().removeIf(u -> !now.isBefore(u));
  }
}
