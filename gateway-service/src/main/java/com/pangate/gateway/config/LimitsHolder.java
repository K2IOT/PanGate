package com.pangate.gateway.config;

import java.util.concurrent.atomic.AtomicReference;

/** Hot-reloadable holder of the current {@link GatewayLimits}. */
public final class LimitsHolder {

  private final AtomicReference<GatewayLimits> current;

  public LimitsHolder(GatewayLimits initial) {
    this.current = new AtomicReference<>(initial);
  }

  public GatewayLimits current() {
    return current.get();
  }

  public void set(GatewayLimits limits) {
    current.set(limits);
  }
}
