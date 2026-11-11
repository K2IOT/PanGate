package com.pangate.auth.application.port;

import java.time.Instant;

/** Wall clock, replaceable in tests. */
@FunctionalInterface
public interface Clock {
  Instant now();
}
