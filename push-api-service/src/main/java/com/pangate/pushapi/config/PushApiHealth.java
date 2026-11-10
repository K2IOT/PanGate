package com.pangate.pushapi.config;

import com.pangate.pushapi.auth.ApiTokenVerifier;
import com.pangate.pushapi.kafka.ControlListener;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Readiness "pushapi" (push-api spec §4.1, §5): {@code gw.control} replayed (no revoked token is accepted by a new pod)
 * and a JWKS loaded.
 */
public final class PushApiHealth implements HealthIndicator {

  private final ControlListener control;
  private final ApiTokenVerifier verifier;

  public PushApiHealth(ControlListener control, ApiTokenVerifier verifier) {
    this.control = control;
    this.verifier = verifier;
  }

  @Override
  public Health health() {
    boolean replayed = control.caughtUp();
    boolean jwks = verifier.loaded();
    return (replayed && jwks ? Health.up() : Health.outOfService()).withDetail("controlReplayed", replayed)
        .withDetail("jwksLoaded", jwks).build();
  }
}
