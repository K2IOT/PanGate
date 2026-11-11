package com.pangate.pushapi.store;

import com.pangate.pushapi.config.PushApiProperties;
import com.pangate.pushapi.core.StoreUnavailableException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Redis state (seq, seqepochs, idempotency, status): standalone or cluster per {@code push-api.redis.topology}. */
public final class RedisState implements AutoCloseable {

  private final RedisClusterAsyncCommands<String, String> async;
  private final AutoCloseable closer;
  private final Duration timeout;
  private final boolean cluster;
  private final Object client;
  private final Object connection;
  private final ClientResources resources;

  /** A master and the slots it serves (push-api spec §3.2). */
  public record Shard(int start, int end, String runId) {
    public String range() {
      return start + "-" + end;
    }
  }

  private RedisState(RedisClusterAsyncCommands<String, String> async, AutoCloseable closer, Duration timeout,
      boolean cluster, Object client, Object connection, ClientResources resources) {
    this.async = async;
    this.closer = closer;
    this.timeout = timeout;
    this.cluster = cluster;
    this.client = client;
    this.connection = connection;
    this.resources = resources;
  }

  public static RedisState connect(PushApiProperties.Redis p) {
    ClientResources res = DefaultClientResources.create();
    RedisURI uri = RedisURI.create(p.uri());
    if ("cluster".equals(p.topology())) {
      RedisClusterClient c = RedisClusterClient.create(res, uri);
      var conn = c.connect();
      return new RedisState(conn.async(), () -> {
        conn.close();
        c.shutdown();
        res.shutdown();
      }, p.timeout(), true, c, conn, res);
    }
    RedisClient c = RedisClient.create(res, uri);
    var conn = c.connect();
    return new RedisState(conn.async(), () -> {
      conn.close();
      c.shutdown();
      res.shutdown();
    }, p.timeout(), false, c, conn, res);
  }

  public RedisClusterAsyncCommands<String, String> async() {
    return async;
  }

  public boolean cluster() {
    return cluster;
  }

  /** The Lettuce client ({@code RedisClient} or {@code RedisClusterClient}). */
  public Object client() {
    return client;
  }

  public ClientResources resources() {
    return resources;
  }

  public Duration timeout() {
    return timeout;
  }

  /** Masters with their slot ranges and run_id; standalone is one shard 0-16383. */
  public java.util.List<Shard> shards() {
    if (!cluster) {
      return java.util.List.of(new Shard(0, 16_383, runId(await(async.info("server"), "INFO server"))));
    }
    RedisClusterClient c = (RedisClusterClient) client;
    @SuppressWarnings("unchecked")
    var conn = (io.lettuce.core.cluster.api.StatefulRedisClusterConnection<String, String>) connection;
    c.refreshPartitions();
    java.util.List<Shard> out = new java.util.ArrayList<>();
    for (var node : c.getPartitions()) {
      if (!node.is(io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag.UPSTREAM)) {
        continue;
      }
      String run = runId(await(conn.getConnection(node.getNodeId()).async().info("server"), "INFO server"));
      java.util.List<Integer> slots = new java.util.ArrayList<>(node.getSlots());
      java.util.Collections.sort(slots);
      int i = 0;
      while (i < slots.size()) {   // contiguous ranges
        int start = slots.get(i);
        int end = start;
        while (i + 1 < slots.size() && slots.get(i + 1) == end + 1) {
          end = slots.get(++i);
        }
        out.add(new Shard(start, end, run));
        i++;
      }
    }
    return out;
  }

  static String runId(String info) {
    for (String line : info.split("\\r?\\n")) {
      if (line.startsWith("run_id:")) {
        return line.substring(7).trim();
      }
    }
    throw new com.pangate.pushapi.core.StoreUnavailableException("INFO server has no run_id", null);
  }

  /** Waits for {@code f} up to the configured timeout. */
  public <T> T await(CompletionStage<T> f, String what) {
    try {
      return f.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StoreUnavailableException(what + ": interrupted", e);
    } catch (Exception e) {
      throw new StoreUnavailableException(what, e);
    }
  }

  @Override
  public void close() {
    try {
      closer.close();
    } catch (Exception ignored) {
      // shutting down
    }
  }
}
