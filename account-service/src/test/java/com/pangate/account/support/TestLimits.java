package com.pangate.account.support;

import com.pangate.account.config.AccountProperties;
import java.time.Duration;

public final class TestLimits {

  /** The spec §7 table. */
  public static final AccountProperties.Limits SPEC = new AccountProperties.Limits(10, 50, 1000, 100,
      Duration.ofMinutes(15), Duration.ofMinutes(15), Duration.ofSeconds(2), 50, 200, 60, 600, 20);

  private TestLimits() {}
}
