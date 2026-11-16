package com.pangate.account.application.port;

/** Counters of account spec §7, kept out of the use cases' types (Micrometer adapter in config). */
public interface Metrics {

  Metrics NONE = (name, tags) -> { };

  /** {@code tags}: key, value, key, value… */
  void count(String name, String... tags);
}
