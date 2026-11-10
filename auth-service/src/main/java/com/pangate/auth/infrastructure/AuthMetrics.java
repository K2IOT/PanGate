package com.pangate.auth.infrastructure;

import com.pangate.auth.application.KeyRing;
import com.pangate.auth.domain.KeyState;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** Metrics of spec §7. Gauges read in-memory values, never the database at scrape time. */
public final class AuthMetrics {

  private final MeterRegistry registry;
  private final AtomicLong outboxPending = new AtomicLong();
  private final AtomicLong outboxOldestAge = new AtomicLong();

  public AuthMetrics(MeterRegistry registry, KeyRing keys) {
    this.registry = registry;
    Gauge.builder("auth.outbox.pending", outboxPending, AtomicLong::get).register(registry);
    Gauge.builder("auth.outbox.oldest_age_seconds", outboxOldestAge, AtomicLong::get).register(registry);
    for (KeyState s : KeyState.values()) {
      Gauge.builder("auth.keys", keys, k -> k.count(s)).tag("state", s.db()).register(registry);
    }
    registry.counter("auth.keys.undecryptable");
  }

  public void issued(String type, String result) {
    registry.counter("auth.tokens.issued", "type", type, "result", result).increment();
  }

  public void latency(String type, long startNanos) {
    Timer.builder("auth.token.latency").tag("type", type).register(registry)
        .record(Duration.ofNanos(System.nanoTime() - startNanos));
  }

  public void revocation(String kind) {
    registry.counter("auth.revocations", "kind", kind).increment();
  }

  public void rateLimited(String endpoint) {
    registry.counter("auth.ratelimited", "endpoint", endpoint).increment();
  }

  public void admin(String action, String result) {
    registry.counter("auth.admin.actions", "action", action, "result", result).increment();
  }

  public void outbox(long pending, long oldestAgeSeconds) {
    outboxPending.set(pending);
    outboxOldestAge.set(oldestAgeSeconds);
  }
}
