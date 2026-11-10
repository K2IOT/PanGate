package com.pangate.router.nodes;

import io.lettuce.core.Range;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@code ZRANGEBYSCORE nodes:live (now-30s) +inf}, then per node {@code GET node:{id}}, {@code GET node:addr:{id}},
 * {@code EXISTS node:draining:{id}} (pipelined). A node counts only with both an epoch and an address.
 */
public final class RedisNodeSource implements NodeSource {

  static final long LIVE_WINDOW_MS = 30_000;

  private final RedisClusterAsyncCommands<String, String> redis;
  private final Duration timeout;

  public RedisNodeSource(RedisClusterAsyncCommands<String, String> redis, Duration timeout) {
    this.redis = redis;
    this.timeout = timeout;
  }

  @Override
  public Map<String, Node> fetch(long nowMs) throws Exception {
    List<String> ids = redis.zrangebyscore("nodes:live", Range.create((double) (nowMs - LIVE_WINDOW_MS),
        Double.POSITIVE_INFINITY)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    record Pending(String id, CompletableFuture<String> epoch, CompletableFuture<String> addr,
        CompletableFuture<Long> draining) {}
    List<Pending> pending = ids.stream().map(id -> new Pending(id,
        redis.get("node:{" + id + "}").toCompletableFuture(),
        redis.get("node:addr:{" + id + "}").toCompletableFuture(),
        redis.exists("node:draining:{" + id + "}").toCompletableFuture())).toList();
    Map<String, Node> out = new HashMap<>();
    for (Pending p : pending) {
      String epoch = p.epoch().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      String addr = p.addr().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      Long draining = p.draining().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (epoch != null && addr != null) {
        try {
          out.put(p.id(), new Node(p.id(), Long.parseLong(epoch), addr, draining != null && draining > 0));
        } catch (NumberFormatException ignored) {
          // not a lease value
        }
      }
    }
    return out;
  }
}
