package com.pangate.gateway.mgmt;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

class HealthIndicatorsTest {

  @Test
  void readinessIndicatorListsReasons() {
    AtomicReference<String> reason = new AtomicReference<>("listeners not bound");
    GatewayReadinessHealthIndicator indicator = new GatewayReadinessHealthIndicator(
        List.of(() -> Optional.ofNullable(reason.get()), Optional::empty));

    Health down = indicator.health();
    assertThat(down.getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    assertThat(down.getDetails().get("reasons")).isEqualTo(List.of("listeners not bound"));

    reason.set(null);
    assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void livenessIndicatorDownWhenLagOver5s() {
    AtomicReference<Double> lag = new AtomicReference<>(0.01);
    EventLoopLagHealthIndicator indicator = new EventLoopLagHealthIndicator(lag::get);

    assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    lag.set(6.0);
    Health h = indicator.health();
    assertThat(h.getStatus()).isEqualTo(Status.DOWN);
    assertThat(h.getDetails().get("maxP99Seconds")).isEqualTo(6.0);
  }
}
