package com.pangate.gateway.auth;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** At most N handshakes per principal per sliding minute on this node (spec §4.6 h); approximate by design. */
public final class PrincipalHandshakeLimiter {

  private static final long WINDOW_MS = 60_000;
  private static final long IDLE_MS = 120_000;
  private static final int SWEEP_EVERY = 1_000;

  private final int perMinute;
  private final LongSupplier nowMillis;
  private final ConcurrentHashMap<String, ArrayDeque<Long>> windows = new ConcurrentHashMap<>();
  private final AtomicLong calls = new AtomicLong();

  public PrincipalHandshakeLimiter(int perMinute, LongSupplier nowMillis) {
    this.perMinute = perMinute;
    this.nowMillis = nowMillis;
  }

  public boolean tryAcquire(String principal) {
    long now = nowMillis.getAsLong();
    if (calls.incrementAndGet() % SWEEP_EVERY == 0) {
      sweep(now);
    }
    ArrayDeque<Long> w = windows.computeIfAbsent(principal, p -> new ArrayDeque<>());
    synchronized (w) {
      while (!w.isEmpty() && w.peekFirst() <= now - WINDOW_MS) {
        w.pollFirst();
      }
      if (w.size() >= perMinute) {
        return false;
      }
      w.addLast(now);
      return true;
    }
  }

  private void sweep(long now) {
    windows.entrySet().removeIf(e -> {
      synchronized (e.getValue()) {
        Long last = e.getValue().peekLast();
        return last == null || last <= now - IDLE_MS;
      }
    });
  }

  public int trackedPrincipals() {
    return windows.size();
  }
}
