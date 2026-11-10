package com.pangate.auth.infrastructure.ratelimit;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/** Per-pod token buckets keyed by (bucket, key), e.g. ("token", client_id) (spec §7). */
public final class TokenBuckets {

  /** {@code rate} tokens per second, at most {@code burst} saved up. */
  public record Limit(double rate, int burst) {}

  static final int MAX_KEYS = 100_000;

  private static final class State {
    double tokens;
    long at;

    State(double tokens, long at) {
      this.tokens = tokens;
      this.at = at;
    }
  }

  private final Map<String, Limit> limits;
  private final LongSupplier nanos;
  private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<>();
  private final int maxKeys;

  public TokenBuckets(Map<String, Limit> limits, LongSupplier nanos) {
    this(limits, nanos, MAX_KEYS);
  }

  public TokenBuckets(Map<String, Limit> limits, LongSupplier nanos, int maxKeys) {
    this.limits = Map.copyOf(limits);
    this.nanos = nanos;
    this.maxKeys = maxKeys;
  }

  /** @return empty if allowed, else the milliseconds until a token is available */
  public OptionalLong tryAcquire(String bucket, String key) {
    Limit l = limits.get(bucket);
    if (l == null || l.rate() <= 0) {
      return OptionalLong.empty();
    }
    long now = nanos.getAsLong();
    String k = bucket + '\0' + key;
    State s = states.get(k);
    if (s == null) {
      if (states.size() >= maxKeys) {
        evictIdle(now);
      }
      // Still full: limit memory, keep every live bucket; the new key gets a bucket that is not remembered.
      s = states.size() >= maxKeys ? new State(l.burst(), now) : states.computeIfAbsent(k, x -> new State(l.burst(), now));
    }
    synchronized (s) {
      s.tokens = Math.min(l.burst(), s.tokens + (now - s.at) / 1e9 * l.rate());
      s.at = now;
      if (s.tokens >= 1) {
        s.tokens -= 1;
        return OptionalLong.empty();
      }
      return OptionalLong.of(Math.max(1, (long) Math.ceil((1 - s.tokens) / l.rate() * 1000)));
    }
  }

  /** Drops buckets that have refilled completely: forgetting them changes nothing. */
  private void evictIdle(long now) {
    states.entrySet().removeIf(e -> {
      Limit l = limits.get(e.getKey().substring(0, e.getKey().indexOf('\0')));
      State s = e.getValue();
      synchronized (s) {
        return l == null || s.tokens + (now - s.at) / 1e9 * l.rate() >= l.burst();
      }
    });
  }
}
