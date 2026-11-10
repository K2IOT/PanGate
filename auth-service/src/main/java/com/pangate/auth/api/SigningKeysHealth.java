package com.pangate.auth.api;

import com.pangate.auth.application.KeyRing;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/** Not ready while no key can sign (spec §6); part of the readiness group. */
@Component("signingKeys")
public class SigningKeysHealth implements HealthIndicator {

  private final KeyRing keys;

  public SigningKeysHealth(KeyRing keys) {
    this.keys = keys;
  }

  @Override
  public Health health() {
    return keys.canSign() ? Health.up().withDetail("published", keys.size()).build()
        : Health.down().withDetail("reason", "no signing key can be decrypted").build();
  }
}
