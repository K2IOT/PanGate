package com.pangate.chat.config;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Readiness "chat" (chat spec §8): {@code gw.control} revocations replayed (a new pod accepts no revoked token), a
 * JWKS loaded, and ScyllaDB answered within the last 10 s.
 */
public final class ChatHealth implements HealthIndicator {

  static final long SCYLLA_WINDOW_MS = 10_000;

  private final BooleanSupplier revocationsReplayed;
  private final BooleanSupplier jwksLoaded;
  private final LongSupplier monoMs;
  private volatile long scyllaOkAt = Long.MIN_VALUE / 2;

  public ChatHealth(BooleanSupplier revocationsReplayed, BooleanSupplier jwksLoaded, LongSupplier monoMs) {
    this.revocationsReplayed = revocationsReplayed;
    this.jwksLoaded = jwksLoaded;
    this.monoMs = monoMs;
  }

  /** Called after each successful ScyllaDB probe. */
  public void scyllaAnswered() {
    scyllaOkAt = monoMs.getAsLong();
  }

  @Override
  public Health health() {
    boolean replayed = revocationsReplayed.getAsBoolean();
    boolean jwks = jwksLoaded.getAsBoolean();
    boolean scylla = monoMs.getAsLong() - scyllaOkAt <= SCYLLA_WINDOW_MS;
    return (replayed && jwks && scylla ? Health.up() : Health.outOfService())
        .withDetail("revocationsReplayed", replayed).withDetail("jwksLoaded", jwks)
        .withDetail("scyllaAnswered", scylla).build();
  }
}
