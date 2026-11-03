package com.pangate.gateway.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.KeyUse;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cached ES256 verification keys (spec §4.9): periodic refresh, whole-set replacement, singleflight emergency
 * refresh for unknown kids (at most once per 10 s), and the last good set kept on any error.
 */
public final class JwksCache {

  private static final Logger log = LoggerFactory.getLogger(JwksCache.class);
  static final Duration MIN_INTERVAL = Duration.ofSeconds(60);
  static final Duration MAX_INTERVAL = Duration.ofHours(1);
  static final Duration RETRY_UNTIL_LOADED = Duration.ofSeconds(10);
  static final long EMERGENCY_MIN_GAP_MS = 10_000;

  private final JwksSource source;
  private final ScheduledExecutorService tick;
  private final Executor offload;
  private final Duration refresh;
  private final LongSupplier nowMillis;
  private final AtomicReference<Map<String, ECPublicKey>> keys = new AtomicReference<>(Map.of());
  private final ReentrantLock fetchLock = new ReentrantLock();
  private final AtomicReference<CompletableFuture<Void>> emergency = new AtomicReference<>();
  private final LongAdder updated = new LongAdder();
  private final LongAdder notModified = new LongAdder();
  private final LongAdder errors = new LongAdder();
  private volatile String etag;
  private volatile long lastSuccessMillis = -1;
  private volatile long lastEmergencyMillis = Long.MIN_VALUE / 2;
  private volatile Duration nextDelay;
  private volatile boolean running;

  public JwksCache(JwksSource source, ScheduledExecutorService tick, Executor offload, Duration refresh,
      LongSupplier nowMillis) {
    this.source = source;
    this.tick = tick;
    this.offload = offload;
    this.refresh = refresh;
    this.nowMillis = nowMillis;
    this.nextDelay = RETRY_UNTIL_LOADED;
  }

  /** Loads immediately, then keeps refreshing on {@link #nextDelay()}. Never blocks the caller on I/O. */
  public void start() {
    running = true;
    scheduleCycle(Duration.ZERO);
  }

  public void stop() {
    running = false;
  }

  private void scheduleCycle(Duration delay) {
    if (!running) {
      return;
    }
    tick.schedule(() -> offload.execute(() -> {
      fetchOnce();
      scheduleCycle(nextDelay);
    }), delay.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Fetches once on the offload executor. */
  public CompletableFuture<Void> refresh() {
    return CompletableFuture.runAsync(this::fetchOnce, offload);
  }

  public Optional<ECPublicKey> key(String kid) {
    return Optional.ofNullable(keys.get().get(kid));
  }

  /** Emergency refresh for an unknown kid; concurrent callers share one fetch; at most one fetch per 10 s. */
  public CompletableFuture<Boolean> refreshFor(String kid) {
    if (key(kid).isPresent()) {
      return CompletableFuture.completedFuture(true);
    }
    CompletableFuture<Void> inflight = emergency.get();
    if (inflight != null && !inflight.isDone()) {
      return inflight.thenApply(v -> key(kid).isPresent());
    }
    long now = nowMillis.getAsLong();
    if (now - lastEmergencyMillis < EMERGENCY_MIN_GAP_MS) {
      return CompletableFuture.completedFuture(false);
    }
    CompletableFuture<Void> mine = new CompletableFuture<>();
    if (!emergency.compareAndSet(inflight, mine)) {
      CompletableFuture<Void> other = emergency.get();
      return other.thenApply(v -> key(kid).isPresent());
    }
    lastEmergencyMillis = now;
    offload.execute(() -> {
      try {
        fetchOnce();
      } finally {
        mine.complete(null);
      }
    });
    return mine.thenApply(v -> key(kid).isPresent());
  }

  private void fetchOnce() {
    fetchLock.lock();
    try {
      JwksSource.FetchResult r = source.fetch(etag);
      if (r instanceof JwksSource.Updated u) {
        keys.set(accept(u));
        etag = u.etag();
        lastSuccessMillis = nowMillis.getAsLong();
        updated.increment();
        nextDelay = interval(u.maxAge());
      } else if (r instanceof JwksSource.NotModified nm) {
        lastSuccessMillis = nowMillis.getAsLong();
        notModified.increment();
        nextDelay = interval(nm.maxAge());
      }
    } catch (Exception e) {
      errors.increment();
      nextDelay = lastSuccessMillis < 0 ? RETRY_UNTIL_LOADED : interval(null);
      log.warn("JWKS refresh from {} failed (keeping {} cached keys): {}", source, keys.get().size(), e.toString());
    } finally {
      fetchLock.unlock();
    }
  }

  private Duration interval(Duration maxAge) {
    if (source.fixedInterval() != null) {
      return source.fixedInterval();
    }
    if (maxAge == null) {
      return refresh;
    }
    if (maxAge.compareTo(MIN_INTERVAL) < 0) {
      return MIN_INTERVAL;
    }
    return maxAge.compareTo(MAX_INTERVAL) > 0 ? MAX_INTERVAL : maxAge;
  }

  private static Map<String, ECPublicKey> accept(JwksSource.Updated u) throws JOSEException {
    Map<String, ECPublicKey> out = new HashMap<>();
    for (JWK jwk : u.set().getKeys()) {
      if (!(jwk instanceof ECKey ec) || !Curve.P_256.equals(ec.getCurve()) || ec.getKeyID() == null
          || (ec.getKeyUse() != null && !KeyUse.SIGNATURE.equals(ec.getKeyUse()))
          || (ec.getAlgorithm() != null && !JWSAlgorithm.ES256.equals(ec.getAlgorithm()))) {
        log.warn("ignoring JWKS key {} (only EC P-256 ES256 signing keys are used)", jwk.getKeyID());
        continue;
      }
      out.put(ec.getKeyID(), ec.toECPublicKey());
    }
    return Map.copyOf(out);
  }

  public int size() {
    return keys.get().size();
  }

  /** Seconds since the last successful fetch (200 or 304), or -1 if never loaded. */
  public long ageSeconds() {
    long last = lastSuccessMillis;
    return last < 0 ? -1 : (nowMillis.getAsLong() - last) / 1000;
  }

  public long refreshCount(String result) {
    return switch (result) {
      case "updated" -> updated.sum();
      case "not_modified" -> notModified.sum();
      case "error" -> errors.sum();
      default -> 0;
    };
  }

  Duration nextDelay() {
    return nextDelay;
  }
}
