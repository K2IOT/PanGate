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

  public TokenBuckets(Map<String, Limit> limits, LongSupplier nanos) {
    this.limits = Map.copyOf(limits);
    this.nanos = nanos;
  }

  /** @return empty if allowed, else the milliseconds until a token is available */
  public OptionalLong tryAcquire(String bucket, String key) {
    Limit l = limits.get(bucket);
    if (l == null || l.rate() <= 0) {
      return OptionalLong.empty();
    }
    if (states.size() > MAX_KEYS) {
      states.clear();  // bounded memory; a reset only forgives callers briefly
    }
    long now = nanos.getAsLong();
    State s = states.computeIfAbsent(bucket + '\0' + key, k -> new State(l.burst(), now));
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
}
