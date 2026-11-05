package com.pangate.gateway.tenant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Node-wide connection count per tenant (spec §4.6 step i). Thread-safe: every loop reserves and releases here. */
public final class TenantConnCounter {

  private final ConcurrentHashMap<String, AtomicInteger> counts = new ConcurrentHashMap<>();

  /** +1 if below {@code quota}; never exceeds it under concurrency. */
  public boolean tryReserve(String tenant, int quota) {
    AtomicInteger c = counts.computeIfAbsent(tenant, t -> new AtomicInteger());
    while (true) {
      int n = c.get();
      if (n >= quota) {
        return false;
      }
      if (c.compareAndSet(n, n + 1)) {
        return true;
      }
    }
  }

  /** +1 beyond the local quota: Redis granted a borrowed slot. */
  public void reserveBorrowed(String tenant) {
    counts.computeIfAbsent(tenant, t -> new AtomicInteger()).incrementAndGet();
  }

  public void release(String tenant) {
    counts.computeIfPresent(tenant, (t, c) -> c.decrementAndGet() <= 0 ? null : c);
  }

  public int count(String tenant) {
    AtomicInteger c = counts.get(tenant);
    return c == null ? 0 : c.get();
  }

  public Map<String, Integer> snapshot() {
    Map<String, Integer> out = new HashMap<>();
    counts.forEach((t, c) -> out.put(t, c.get()));
    return out;
  }
}
