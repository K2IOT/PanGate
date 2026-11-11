package com.pangate.router.delivery;

import java.time.Duration;

/** Retry pauses; replaceable in tests. */
@FunctionalInterface
public interface Sleeper {
  void sleep(Duration d) throws InterruptedException;
}
