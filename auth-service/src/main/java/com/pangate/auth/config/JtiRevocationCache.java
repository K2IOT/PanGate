package com.pangate.auth.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-jti revocation answers cached for {@code ttl} (spec §4.2: 10 s), bounded LRU. Only jtis actually presented are
 * looked up, so a tenant with millions of jti revocations costs one indexed query per token per 10 s, not a full load.
 * A failed lookup answers with the last known value (or "not revoked"): the endpoints behind need the database anyway.
 */
public final class JtiRevocationCache {

  private static final Logger log = LoggerFactory.getLogger(JtiRevocationCache.class);

  private record Entry(boolean revoked, long at) {}

  private final Predicate<String> lookup;
  private final long ttlNanos;
  private final LongSupplier nanos;
  private final Map<String, Entry> entries;
  private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();

  public JtiRevocationCache(Predicate<String> lookup, Duration ttl, int maxEntries, LongSupplier nanos) {
    this.lookup = lookup;
    this.ttlNanos = ttl.toNanos();
    this.nanos = nanos;
    this.entries = new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, Entry> e) {
        return size() > maxEntries;
      }
    };
  }

  public boolean isRevoked(String jti) {
    long now = nanos.getAsLong();
    Entry e;
    lock.lock();
    try {
      e = entries.get(jti);
    } finally {
      lock.unlock();
    }
    if (e != null && now - e.at() <= ttlNanos) {
      return e.revoked();
    }
    boolean revoked;
    try {
      revoked = lookup.test(jti);  // outside the lock: no virtual-thread pinning, no head-of-line blocking
    } catch (RuntimeException ex) {
      log.warn("jti revocation lookup failed, using the last known answer: {}", ex.toString());
      return e != null && e.revoked();
    }
    lock.lock();
    try {
      entries.put(jti, new Entry(revoked, now));
    } finally {
      lock.unlock();
    }
    return revoked;
  }

  int size() {
    lock.lock();
    try {
      return entries.size();
    } finally {
      lock.unlock();
    }
  }
}
