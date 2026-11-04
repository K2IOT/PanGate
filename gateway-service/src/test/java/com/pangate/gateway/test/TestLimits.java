package com.pangate.gateway.test;

import com.pangate.gateway.config.GatewayLimits;
import com.pangate.gateway.config.LimitsHolder;
import java.time.Duration;

/** Production limit values (spec §9) for unit tests. */
public final class TestLimits {

  private TestLimits() {}

  public static GatewayLimits defaults() {
    return new GatewayLimits(700_000, 550_000, 200, 65_536, 8_192, Duration.ofSeconds(5), Duration.ofSeconds(10),
        20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100, 32, 65_536, Duration.ofSeconds(5), 10_000);
  }

  public static LimitsHolder holder() {
    return new LimitsHolder(defaults());
  }
}
