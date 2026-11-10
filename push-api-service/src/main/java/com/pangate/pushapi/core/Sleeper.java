package com.pangate.pushapi.core;

import java.time.Duration;

/** Waits; replaceable in tests. */
@FunctionalInterface
public interface Sleeper {
  void sleep(Duration d) throws InterruptedException;
}
