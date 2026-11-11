package com.pangate.pushapi.core;

import java.time.Duration;
import java.util.Optional;

/** Redis state for idempotency keys; every call may throw {@link StoreUnavailableException}. */
public interface IdemStore {

  Optional<String> get(String key);

  boolean setIfAbsent(String key, String value, Duration ttl);

  void set(String key, String value, Duration ttl);

  boolean deleteIfEquals(String key, String value);
}
