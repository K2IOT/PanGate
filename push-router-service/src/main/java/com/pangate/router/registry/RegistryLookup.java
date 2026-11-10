package com.pangate.router.registry;

import com.pangate.router.delivery.Connections;
import com.pangate.router.nodes.Node;
import com.pangate.router.nodes.NodeDirectory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * {@code HGETALL reg:{p}} → live connections (push contracts spec §4, push-router spec §1): values v1/v2 parsed like
 * the testkit's RegistryValues, kept only if their node is live, not draining and at the same epoch. Raw hashes are
 * cached for {@code cache}; the epoch filter runs on every call against the current directory.
 */
public final class RegistryLookup implements Connections {

  private record Cached(Map<String, String> raw, long atMono) {}

  private final Function<String, CompletionStage<Map<String, String>>> hgetall;
  private final NodeDirectory dir;
  private final long cacheMs;
  private final Duration timeout;
  private final LongSupplier monoMs;
  private final ConcurrentHashMap<String, Cached> cache = new ConcurrentHashMap<>();

  public RegistryLookup(Function<String, CompletionStage<Map<String, String>>> hgetall, NodeDirectory dir,
      Duration cache, Duration timeout, LongSupplier monoMs) {
    this.hgetall = hgetall;
    this.dir = dir;
    this.cacheMs = cache.toMillis();
    this.timeout = timeout;
    this.monoMs = monoMs;
  }

  @Override
  public List<LiveConnection> live(String principal) {
    long now = monoMs.getAsLong();
    Cached c = cache.get(principal);
    if (c == null || now - c.atMono() > cacheMs) {
      Map<String, String> raw;
      try {
        raw = hgetall.apply(principal).toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RegistryUnavailableException(Integer.toHexString(principal.hashCode()), e);
      } catch (Exception e) {
        throw new RegistryUnavailableException(Integer.toHexString(principal.hashCode()), e);
      }
      c = new Cached(raw == null ? Map.of() : Map.copyOf(raw), now);
      if (cache.size() > 100_000) {
        cache.clear();
      }
      cache.put(principal, c);
    }
    List<LiveConnection> out = new ArrayList<>();
    c.raw().forEach((connId, value) -> parse(connId, value).ifPresent(l -> {
      Optional<Node> n = dir.node(l.nodeId());
      if (n.isPresent() && n.get().epoch() == l.epoch() && !n.get().draining()) {
        out.add(l);
      }
    }));
    return out;
  }

  @Override
  public void invalidate(String principal) {
    cache.remove(principal);
  }

  /** Same grammar as RegistryValues.parse: split with limit 5, ASCII digits, non-empty node. */
  static Optional<LiveConnection> parse(String connId, String value) {
    if (value == null) {
      return Optional.empty();
    }
    String[] p = value.split("\\|", 5);
    if (p.length < 4 || p[0].isEmpty() || !digits(p[1]) || !digits(p[2])) {
      return Optional.empty();
    }
    try {
      return Optional.of(new LiveConnection(connId, p[0], Long.parseLong(p[1]),
          p.length == 5 ? Optional.of(p[4]) : Optional.empty()));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  private static boolean digits(String s) {
    if (s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) < '0' || s.charAt(i) > '9') {
        return false;
      }
    }
    return true;
  }
}
