package com.pangate.gateway.metrics;

import com.pangate.gateway.auth.JwksCache;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.protocol.CloseCode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ConcurrentHashMap;

/** Gateway meters (spec §8). Labels are bounded: result, reason, code, loop — never per connection. */
public final class GatewayMetrics {

  private final MeterRegistry registry;
  private final ConcurrentHashMap<String, Counter> handshakes = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Counter> rejections = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<CloseCode, Counter> closes = new ConcurrentHashMap<>();

  public GatewayMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  /** result ∈ ok, auth_failed, expired, wrong_cell, rate_limited, bad_frame, timeout. */
  public void handshake(String result) {
    handshakes.computeIfAbsent(result, r -> Counter.builder("gw.handshakes").tag("result", r).register(registry))
        .increment();
  }

  /** reason ∈ draining, hard_cap, per_ip, rate. */
  public void admissionRejected(String reason) {
    rejections.computeIfAbsent(reason,
        r -> Counter.builder("gw.admission.rejected").tag("reason", r).register(registry)).increment();
  }

  public void closed(CloseCode code) {
    closes.computeIfAbsent(code,
        c -> Counter.builder("gw.closes").tag("code", Integer.toString(c.code())).register(registry)).increment();
  }

  public void bindLoops(LoopRegistry loops) {
    for (LoopContext c : loops.all()) {
      Gauge.builder("gw.connections.active", c, LoopContext::activeCount)
          .tag("loop", Integer.toString(c.index())).register(registry);
    }
  }

  public void bindJwks(JwksCache jwks) {
    Gauge.builder("gw.jwks.keys", jwks, JwksCache::size).register(registry);
    Gauge.builder("gw.jwks.age", jwks, JwksCache::ageSeconds).baseUnit("seconds").register(registry);
    for (String result : new String[] {"updated", "not_modified", "error"}) {
      FunctionCounter.builder("gw.jwks.refresh", jwks, j -> j.refreshCount(result))
          .tag("result", result).register(registry);
    }
  }

  /**
   * A counter registered once; hot-path callers keep the returned instance. Tags are alternating key, value pairs
   * with bounded values (spec §8).
   */
  public Counter counter(String name, String... tags) {
    return Counter.builder(name).tags(tags).register(registry);
  }

  public <T> void gauge(String name, T obj, java.util.function.ToDoubleFunction<T> value) {
    Gauge.builder(name, obj, value).register(registry);
  }

  public MeterRegistry registry() {
    return registry;
  }
}
