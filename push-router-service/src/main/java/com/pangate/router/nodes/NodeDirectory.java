package com.pangate.router.nodes;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Snapshot of the cell's live gateways, refreshed every directory-refresh (push-router spec §3.2). */
public final class NodeDirectory {

  private static final Logger log = LoggerFactory.getLogger(NodeDirectory.class);

  private final NodeSource source;
  private final LongSupplier wallMs;
  private final LongSupplier monoMs;
  private final long staleAfterMs;
  private final List<BiConsumer<Map<String, Node>, Map<String, Node>>> listeners = new CopyOnWriteArrayList<>();
  private volatile Map<String, Node> snapshot = Map.of();
  private volatile long lastOkMono = Long.MIN_VALUE;

  public NodeDirectory(NodeSource source, LongSupplier wallMs, LongSupplier monoMs, Duration staleAfter) {
    this.source = source;
    this.wallMs = wallMs;
    this.monoMs = monoMs;
    this.staleAfterMs = staleAfter.toMillis();
  }

  /** One refresh; on failure the previous snapshot stays. Listeners get (old, new) after a success. */
  public synchronized void refresh() {
    Map<String, Node> next;
    try {
      next = Map.copyOf(source.fetch(wallMs.getAsLong()));
    } catch (Exception e) {
      log.warn("node directory not refreshed, keeping {} nodes: {}", snapshot.size(), e.toString());
      return;
    }
    Map<String, Node> old = snapshot;
    snapshot = next;
    lastOkMono = monoMs.getAsLong();
    for (var l : listeners) {
      try {
        l.accept(old, next);
      } catch (RuntimeException e) {
        log.warn("node directory listener failed: {}", e.toString());
      }
    }
  }

  public Map<String, Node> live() {
    return snapshot;
  }

  /** The snapshot, or a transient failure when it is stale: a blind router must not decide outcomes (spec §4). */
  public Map<String, Node> requireFresh() {
    if (!fresh()) {
      throw new com.pangate.router.registry.RegistryUnavailableException("-",
          new IllegalStateException("node directory not refreshed for more than " + staleAfterMs + " ms"));
    }
    return snapshot;
  }

  public Optional<Node> node(String id) {
    return Optional.ofNullable(snapshot.get(id));
  }

  /** The last successful refresh is at most staleAfter old (readiness, spec §4). */
  public boolean fresh() {
    return lastOkMono != Long.MIN_VALUE && monoMs.getAsLong() - lastOkMono <= staleAfterMs;
  }

  public void onChange(BiConsumer<Map<String, Node>, Map<String, Node>> listener) {
    listeners.add(listener);
  }
}
