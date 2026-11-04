package com.pangate.gateway.upstream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Per-principal token bucket for {@code POST /v1/upstream} (no session, so §5.5's per-session limiter cannot apply):
 * the tenant's {@code inbound-rate} tokens per second, {@code inbound-burst} deep. Node-local; idle buckets are
 * evicted by {@link #evictIdle()}. Thread-safe.
 */
public final class PrincipalRateLimiter {

  static final long IDLE_NANOS = 60_000_000_000L;

  private static final class Bucket {
    double tokens;
    long lastNanos;

    Bucket(double tokens, long now) {
      this.tokens = tokens;
      this.lastNanos = now;
    }
  }

  private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();
  private final LongSupplier nanos;

  public PrincipalRateLimiter(LongSupplier nanos) {
    this.nanos = nanos;
  }

  public boolean tryAcquire(String principal, int ratePerSec, int burst) {
    if (ratePerSec <= 0) {
      return true;
    }
    long now = nanos.getAsLong();
    Bucket b = buckets.computeIfAbsent(principal, p -> new Bucket(burst, now));
    synchronized (b) {
      b.tokens = Math.min(burst, b.tokens + (now - b.lastNanos) / 1e9 * ratePerSec);
      b.lastNanos = now;
      if (b.tokens >= 1) {
        b.tokens -= 1;
        return true;
      }
      return false;
    }
  }

  /** Drops buckets untouched for 60 s (they would be full again anyway). */
  public void evictIdle() {
    long now = nanos.getAsLong();
    buckets.entrySet().removeIf(e -> {
      synchronized (e.getValue()) {
        return now - e.getValue().lastNanos > IDLE_NANOS;
      }
    });
  }

  int size() {
    return buckets.size();
  }
}
