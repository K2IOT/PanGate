package com.pangate.pushapi.core;

import com.pangate.contracts.v1.PushRequest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Token buckets per tenant × kind on this pod (push-api spec §2 step 5, ADR 0026). MESSAGE uses push-rate /
 * push-burst, BROADCAST broadcast-rate with burst = rate; CONTROL is never limited.
 */
public final class RateLimiter {

  private static final class Bucket {
    double tokens = -1;
    long last;
  }

  private final LongSupplier monoNanos;
  private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

  public RateLimiter(LongSupplier monoNanos) {
    this.monoNanos = monoNanos;
  }

  /** 0 when granted, else the nanoseconds until one token is available. */
  public long tryAcquire(String tenant, PushRequest.Kind kind, TenantPolicy p) {
    int rate;
    int burst;
    switch (kind) {
      case KIND_MESSAGE -> {
        rate = p.pushRate();
        burst = Math.max(p.pushBurst(), 1);
      }
      case KIND_BROADCAST -> {
        rate = p.broadcastRate();
        burst = Math.max(p.broadcastRate(), 1);
      }
      default -> {
        return 0;
      }
    }
    Bucket b = buckets.computeIfAbsent(tenant + "|" + kind.getNumber(), k -> new Bucket());
    synchronized (b) {
      long now = monoNanos.getAsLong();
      if (b.tokens < 0) {
        b.tokens = burst;
      } else {
        b.tokens = Math.min(burst, b.tokens + (now - b.last) * (double) rate / 1e9);
      }
      b.last = now;
      if (b.tokens >= 1) {
        b.tokens -= 1;
        return 0;
      }
      return rate <= 0 ? Long.MAX_VALUE : (long) Math.ceil((1 - b.tokens) * 1e9 / rate);
    }
  }
}
