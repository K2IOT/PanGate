package com.pangate.gateway.mgmt;

import java.util.function.DoubleSupplier;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/** Health group "liveness" → GET /live: DOWN when any event loop's p99 lag reaches 5 s. */
public final class EventLoopLagHealthIndicator implements HealthIndicator {

  static final double MAX_LAG_SECONDS = 5.0;

  private final DoubleSupplier maxP99Seconds;

  public EventLoopLagHealthIndicator(DoubleSupplier maxP99Seconds) {
    this.maxP99Seconds = maxP99Seconds;
  }

  @Override
  public Health health() {
    double lag = maxP99Seconds.getAsDouble();
    return (lag < MAX_LAG_SECONDS ? Health.up() : Health.down()).withDetail("maxP99Seconds", lag).build();
  }
}
