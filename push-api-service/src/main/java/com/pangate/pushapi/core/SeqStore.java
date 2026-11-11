package com.pangate.pushapi.core;

/** {@code INCR} on Redis state (push contracts §3.3). */
@FunctionalInterface
public interface SeqStore {
  /** @throws StoreUnavailableException on timeout or error */
  long incr(String key);
}
