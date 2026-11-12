package com.pangate.chat.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * In-memory token buckets per key (chat spec §4.3: per pod, like Push API, ADR 0026). A bucket starts full. Buckets
 * that refilled completely are dropped once the map grows past {@link #SWEEP_AT}, so idle users cost nothing.
 */
public final class TokenBucketLimiter {

  static final int SWEEP_AT = 100_000;

  private static final class Bucket {
    double tokens;
    long atNanos;

    Bucket(double tokens, long atNanos) {
      this.tokens = tokens;
      this.atNanos = atNanos;
    }
  }

  private final double perNano;
  private final int burst;
  private final LongSupplier nanos;
  private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

  public TokenBucketLimiter(int rate, int burst, LongSupplier nanos) {
    this.perNano = rate / 1e9;
    this.burst = burst;
    this.nanos = nanos;
  }

  public boolean tryAcquire(String key) {
    long now = nanos.getAsLong();
    if (buckets.size() > SWEEP_AT) {
      sweep(now);
    }
    Bucket b = buckets.computeIfAbsent(key, k -> new Bucket(burst, now));
    synchronized (b) {
      refill(b, now);
      if (b.tokens < 1) {
        return false;
      }
      b.tokens -= 1;
      return true;
    }
  }

  private void refill(Bucket b, long now) {
    if (now > b.atNanos) {
      b.tokens = Math.min(burst, b.tokens + (now - b.atNanos) * perNano);
      b.atNanos = now;
    }
  }

  private void sweep(long now) {
    buckets.entrySet().removeIf(e -> {
      Bucket b = e.getValue();
      synchronized (b) {
        refill(b, now);
        return b.tokens >= burst;
      }
    });
  }

  int size() {
    return buckets.size();
  }
}
