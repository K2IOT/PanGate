package com.pangate.notify.application;

import com.pangate.notify.application.port.RateLimiter;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.DomainException;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.domain.PushToken;
import com.pangate.notify.domain.TokenRules;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;

/** Offline notifications spec §5.1: a device sets or removes its own push token. */
public final class Registrations {

  private final TokenStore store;
  private final RateLimiter limiter;
  private final int perMinute;
  private final Clock clock;
  private final MeterRegistry meters;

  public Registrations(TokenStore store, RateLimiter limiter, int perMinute, Clock clock, MeterRegistry meters) {
    this.store = store;
    this.limiter = limiter;
    this.perMinute = perMinute;
    this.clock = clock;
    this.meters = meters;
  }

  public void register(Caller c, Platform p, String token, String environment) {
    String env = environment == null || environment.isEmpty() ? TokenRules.PRODUCTION : environment;
    TokenRules.validate(p, token, env);
    limit(c);
    int moved = store.register(new PushToken(c.tenantId(), c.principalId(), c.deviceId(), p, env, token,
        clock.instant()));
    if (moved > 0) {
      meters.counter("notify.tokens.removed", "reason", "replaced").increment(moved);
    }
  }

  public void unregister(Caller c) {
    limit(c);
    store.remove(c.tenantId(), c.principalId(), c.deviceId());
  }

  private void limit(Caller c) {
    if (!limiter.tryAcquire(c.principalId() + "/" + c.deviceId(), perMinute)) {
      throw new DomainException("rate_limited", "too many registrations from this device; try again in a minute");
    }
  }
}
