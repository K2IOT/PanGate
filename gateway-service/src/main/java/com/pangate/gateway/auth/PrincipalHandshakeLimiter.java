package com.pangate.gateway.auth;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/** At most N handshakes per principal per sliding minute on this node (spec §4.6 h); approximate by design. */
public final class PrincipalHandshakeLimiter {

  private static final long WINDOW_MS = 60_000;
  private static final long IDLE_MS = 120_000;

  private final int perMinute;
  private final LongSupplier nowMillis;
  private final ConcurrentHashMap<String, ArrayDeque<Long>> windows = new ConcurrentHashMap<>();

  public PrincipalHandshakeLimiter(int perMinute, LongSupplier nowMillis) {
    this.perMinute = perMinute;
    this.nowMillis = nowMillis;
  }

  public boolean tryAcquire(String principal) {
    long now = nowMillis.getAsLong();
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

  /** Forgets principals idle for 120 s. Runs on gw-tick (every 30 s), never on an event loop. */
  public void sweepIdle() {
    long now = nowMillis.getAsLong();
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
