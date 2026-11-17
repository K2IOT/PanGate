package com.pangate.notify.application;

import com.pangate.notify.application.port.TokenStore;
import java.time.Clock;
import java.time.Duration;

/** Offline notifications spec §5.1: a token not registered again for the idle TTL (180 days) goes. */
public final class PurgeIdleTokens {

  private final TokenStore store;
  private final Duration ttl;
  private final Clock clock;

  public PurgeIdleTokens(TokenStore store, Duration ttl, Clock clock) {
    this.store = store;
    this.ttl = ttl;
    this.clock = clock;
  }

  public int run() {
    return store.purgeIdle(clock.instant().minus(ttl));
  }
}
