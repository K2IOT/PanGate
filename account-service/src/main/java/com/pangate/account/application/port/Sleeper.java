package com.pangate.account.application.port;

import java.time.Duration;

/** The slow-down of account spec §7 (a login failed 100 times from many addresses). */
public interface Sleeper {

  Sleeper REAL = d -> {
    try {
      Thread.sleep(d);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  };

  void sleep(Duration d);
}
