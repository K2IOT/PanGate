package com.pangate.auth.infrastructure.ratelimit;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/** More than {@code perMinute} failed client authentications from one source within a minute block it (spec §7). */
public final class FailedAuthTracker {

  private static final long MINUTE = Duration.ofMinutes(1).toNanos();

  private static final class Window {
    final ArrayDeque<Long> failures = new ArrayDeque<>();
    boolean blocked;
    long blockedUntil;
  }

  private final int perMinute;
  private final long blockNanos;
  private final LongSupplier nanos;
  private final ConcurrentHashMap<String, Window> bySource = new ConcurrentHashMap<>();
  private final int maxKeys;

  public FailedAuthTracker(int perMinute, Duration block, LongSupplier nanos) {
    this(perMinute, block, nanos, TokenBuckets.MAX_KEYS);
  }

  public FailedAuthTracker(int perMinute, Duration block, LongSupplier nanos, int maxKeys) {
    this.perMinute = perMinute;
    this.blockNanos = block.toNanos();
    this.nanos = nanos;
    this.maxKeys = maxKeys;
  }

  public void recordFailure(String source) {
    long now = nanos.getAsLong();
    Window w = bySource.get(source);
    if (w == null) {
      if (bySource.size() >= maxKeys) {
        evictIdle(now);
      }
      if (bySource.size() >= maxKeys) {
        return;  // full of live sources: never drop a block to make room for a newcomer
      }
      w = bySource.computeIfAbsent(source, k -> new Window());
    }
    synchronized (w) {
      while (!w.failures.isEmpty() && now - w.failures.peekFirst() > MINUTE) {
        w.failures.pollFirst();
      }
      w.failures.addLast(now);
      if (w.failures.size() > perMinute) {
        w.blocked = true;
        w.blockedUntil = now + blockNanos;
        w.failures.clear();
      }
    }
  }

  /** Drops sources that are not blocked and have no failure within the last minute. */
  private void evictIdle(long now) {
    bySource.values().removeIf(w -> {
      synchronized (w) {
        boolean blocked = w.blocked && w.blockedUntil - now > 0;
        boolean recent = !w.failures.isEmpty() && now - w.failures.peekLast() <= MINUTE;
        return !blocked && !recent;
      }
    });
  }

  /** @return milliseconds the source stays blocked, or empty */
  public OptionalLong blockedFor(String source) {
    Window w = bySource.get(source);
    if (w == null) {
      return OptionalLong.empty();
    }
    long left;
    synchronized (w) {
      if (!w.blocked) {
        return OptionalLong.empty();
      }
      left = w.blockedUntil - nanos.getAsLong();  // no overflow: blockedUntil was set from the same clock
    }
    return left > 0 ? OptionalLong.of(Math.max(1, left / 1_000_000)) : OptionalLong.empty();
  }
}
