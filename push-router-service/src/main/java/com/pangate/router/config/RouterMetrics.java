package com.pangate.router.config;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.delivery.DeliveryResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** The router's meters (push-router spec §5). */
public final class RouterMetrics {

  private final MeterRegistry registry;
  private final AtomicInteger inflight = new AtomicInteger();

  public RouterMetrics(MeterRegistry registry) {
    this.registry = registry;
    registry.gauge("router.inflight", inflight);
  }

  private static String lower(Enum<?> e, String prefix) {
    String n = e.name();
    return (n.startsWith(prefix) ? n.substring(prefix.length()) : n).toLowerCase(Locale.ROOT);
  }

  public void record(PushRecord.Kind kind, DeliveryResult result) {
    registry.counter("router.records", "kind", lower(kind, "KIND_"), "result", lower(result, "")).increment();
  }

  public void invalid() {
    registry.counter("router.records", "kind", "unknown", "result", "invalid").increment();
  }

  public void latency(DeliveryClass c, long ms) {
    Timer.builder("router.delivery.latency").tag("class", lower(c, "DELIVERY_CLASS_")).register(registry)
        .record(Math.max(0, ms), TimeUnit.MILLISECONDS);
  }

  public void offline(OfflineRecord.Reason reason) {
    registry.counter("router.offline", "reason", lower(reason, "REASON_")).increment();
  }

  public void nodeDown() {
    registry.counter("router.node_down").increment();
  }

  public void registryLatency(long nanos) {
    registry.timer("router.registry.latency").record(nanos, TimeUnit.NANOSECONDS);
  }

  public void links(Supplier<Number> open) {
    registry.gauge("router.links", java.util.List.of(io.micrometer.core.instrument.Tag.of("state", "open")), open,
        s -> s.get().doubleValue());
  }

  public AtomicInteger inflight() {
    return inflight;
  }
}
