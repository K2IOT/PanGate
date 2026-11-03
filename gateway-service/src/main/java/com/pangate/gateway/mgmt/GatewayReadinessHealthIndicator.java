package com.pangate.gateway.mgmt;

import java.util.List;
import java.util.Optional;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/** Health group "readiness" → GET /ready: UP only when every {@link ReadinessCheck} passes. */
public final class GatewayReadinessHealthIndicator implements HealthIndicator {

  private final List<ReadinessCheck> checks;

  public GatewayReadinessHealthIndicator(List<ReadinessCheck> checks) {
    this.checks = List.copyOf(checks);
  }

  @Override
  public Health health() {
    List<String> reasons = checks.stream().map(ReadinessCheck::notReadyReason).flatMap(Optional::stream).toList();
    return reasons.isEmpty() ? Health.up().build() : Health.outOfService().withDetail("reasons", reasons).build();
  }
}
