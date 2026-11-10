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
  private final ClientResources resources;

  private RedisState(RedisClusterAsyncCommands<String, String> async, AutoCloseable closer, Duration timeout,
      boolean cluster, Object client, ClientResources resources) {
    this.async = async;
    this.closer = closer;
    this.timeout = timeout;
    this.cluster = cluster;
    this.client = client;
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
      }, p.timeout(), true, c, res);
    }
    RedisClient c = RedisClient.create(res, uri);
    var conn = c.connect();
    return new RedisState(conn.async(), () -> {
      conn.close();
      c.shutdown();
      res.shutdown();
    }, p.timeout(), false, c, res);
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
