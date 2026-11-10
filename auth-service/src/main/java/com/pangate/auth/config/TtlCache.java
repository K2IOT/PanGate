package com.pangate.auth.config;

import java.time.Duration;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A value reloaded at most every {@code ttl}; on a failed reload the previous value is kept. */
public final class TtlCache<T> implements Supplier<T> {

  private static final Logger log = LoggerFactory.getLogger(TtlCache.class);

  private final Supplier<T> loader;
  private final long ttlNanos;
  private final LongSupplier nanos;
  private volatile T value;
  private volatile long loadedAt;

  public TtlCache(Supplier<T> loader, Duration ttl, T initial, LongSupplier nanos) {
    this.loader = loader;
    this.ttlNanos = ttl.toNanos();
    this.nanos = nanos;
    this.value = initial;
    this.loadedAt = nanos.getAsLong() - ttlNanos - 1;
  }

  @Override
  public T get() {
    long now = nanos.getAsLong();
    if (now - loadedAt > ttlNanos) {
      synchronized (this) {
        if (now - loadedAt > ttlNanos) {
          try {
            value = loader.get();
          } catch (RuntimeException e) {
            log.warn("cache reload failed, keeping the previous value: {}", e.toString());
          }
          loadedAt = now;
        }
      }
    }
    return value;
  }

  /** Forces the next {@link #get()} to reload. */
  public void invalidate() {
    loadedAt = nanos.getAsLong() - ttlNanos - 1;
  }
}
