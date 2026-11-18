package com.pangate.notify.application;

import com.pangate.contracts.testkit.NotifyRules;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.notify.application.port.Dedup;
import com.pangate.notify.application.port.Delayer;
import com.pangate.notify.application.port.PushProvider;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.domain.PushToken;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline notifications spec §5.4: one NotifyRequest to every token of its recipient. Order: rules, expiry, token
 * lookup, dedup, send. The dedup mark comes after the lookup, so a store failure (thrown, the batch rewinds) never
 * leaves a request marked but unsent. A store failure while removing a dead token is logged, not thrown.
 */
public final class NotifyDispatcher {

  private static final Logger log = LoggerFactory.getLogger(NotifyDispatcher.class);
  static final long FIRST_PAUSE_MS = 1_000;
  static final long SECOND_PAUSE_MS = 4_000;
  /** Review of CH4: the longest provider Retry-After honoured; one throttle must not hold the whole batch. */
  static final long MAX_RETRY_AFTER_MS = 5_000;

  private final TokenStore store;
  private final Dedup dedup;
  private final BiFunction<String, Platform, PushProvider> providers;
  private final Delayer delayer;
  private final MeterRegistry meters;
  private final int attempts;

  /** {@code providers} returns null when the tenant has no configuration for that platform. */
  public NotifyDispatcher(TokenStore store, Dedup dedup, BiFunction<String, Platform, PushProvider> providers,
      Delayer delayer, MeterRegistry meters, int attempts) {
    this.store = store;
    this.dedup = dedup;
    this.providers = providers;
    this.delayer = delayer;
    this.meters = meters;
    this.attempts = attempts;
  }

  /** Completes with the result for {@code notify.requests{result}}; exceptionally only when the store is down. */
  public CompletableFuture<String> handle(NotifyRequest r, long nowMs) {
    String result;
    try {
      result = route(r, nowMs);
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
    if (result != null) {
      return CompletableFuture.completedFuture(count(result));
    }
    List<PushToken> tokens = r.getTargetCase() == NotifyRequest.TargetCase.DEVICE
        ? store.byDevice(r.getTenantId(), r.getDevice().getPrincipalId(), r.getDevice().getDeviceId())
            .map(List::of).orElse(List.of())
        : store.byPrincipal(r.getTenantId(), r.getPrincipalId());
    if (tokens.isEmpty()) {
      return CompletableFuture.completedFuture(count("no_token"));
    }
    if (!first(r.getRequestId())) {
      return CompletableFuture.completedFuture(count("duplicate"));
    }
    List<CompletableFuture<Void>> sends = new ArrayList<>();
    for (PushToken t : tokens) {
      PushProvider p = providers.apply(t.tenantId(), t.platform());
      if (p != null) {
        sends.add(send(p, t, r, 0, nowMs));
      }
    }
    if (sends.isEmpty()) {
      return CompletableFuture.completedFuture(count("no_config"));
    }
    return CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).thenApply(v -> count("sent"));
  }

  /** A result that ends the request before any token is looked up, or null to go on. */
  private static String route(NotifyRequest r, long nowMs) {
    if (!NotifyRules.violations(r).isEmpty()) {
      return "invalid";
    }
    if (nowMs >= r.getExpiresAtMs()) {
      return "expired";
    }
    return null;
  }

  private boolean first(String id) {
    try {
      return dedup.firstTime(id);
    } catch (RuntimeException e) {
      log.warn("dedup unavailable, sending anyway: {}", e.toString());   // a duplicate beats a lost notification
      return true;
    }
  }

  private CompletableFuture<Void> send(PushProvider p, PushToken t, NotifyRequest r, int attempt, long atMs) {
    long start = System.nanoTime();
    return p.send(t, r).thenCompose(res -> {
      meters.timer("notify.send.latency", "platform", t.platform().wire())
          .record(java.time.Duration.ofNanos(System.nanoTime() - start));
      switch (res) {
        case SendResult.Ok ok -> sendCount(t, "ok");
        case SendResult.TokenDead dead -> removeDead(t);
        case SendResult.AuthFailed af -> {
          sendCount(t, "auth_failed");
          meters.counter("notify.provider.auth_failed", "platform", t.platform().wire()).increment();
          log.error("{} refused our credentials for tenant {}", t.platform().wire(), t.tenantId());
        }
        case SendResult.Rejected rej -> {
          sendCount(t, "rejected");
          log.warn("{} rejected {} for {}: {}", t.platform().wire(), r.getRequestId(), t.deviceId(), rej.reason());
        }
        case SendResult.Transient tr -> {
          long pause = tr.retryAfterMs() > 0 ? Math.min(MAX_RETRY_AFTER_MS, tr.retryAfterMs())
              : attempt == 0 ? FIRST_PAUSE_MS : SECOND_PAUSE_MS;
          if (attempt + 1 < attempts && atMs + pause < r.getExpiresAtMs()) {
            return delayer.after(pause).thenCompose(v -> send(p, t, r, attempt + 1, atMs + pause));
          }
          sendCount(t, "failed");
        }
      }
      return CompletableFuture.completedFuture(null);
    });
  }

  private void removeDead(PushToken t) {
    sendCount(t, "token_dead");
    try {
      if (store.removeIfToken(t.tenantId(), t.principalId(), t.deviceId(), t.token())) {
        meters.counter("notify.tokens.removed", "reason", "provider").increment();
      }
    } catch (RuntimeException e) {
      log.warn("dead token of {} not removed: {}", t.deviceId(), e.toString());
    }
  }

  private void sendCount(PushToken t, String result) {
    meters.counter("notify.sends", "platform", t.platform().wire(), "result", result).increment();
  }

  private String count(String result) {
    meters.counter("notify.requests", "result", result).increment();
    return result;
  }
}
