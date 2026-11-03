package com.pangate.gateway.lifecycle;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.config.RedisTopology;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Registry and state Redis connections (spec §1, §3.1): one multiplexed async connection per endpoint, shared when
 * both URIs are equal (local, dev). Commands are rejected while disconnected instead of queueing on the heap, so
 * callers fail fast into their retry path.
 */
public final class RedisConnections implements AutoCloseable {

  private static final int REQUEST_QUEUE = 10_000;

  private final GatewayProperties.Redis cfg;
  private final List<AutoCloseable> closeables = new ArrayList<>();
  private ClientResources resources;
  private volatile RedisClusterAsyncCommands<String, String> registry;
  private volatile RedisClusterAsyncCommands<String, String> state;

  public RedisConnections(GatewayProperties.Redis cfg) {
    this.cfg = cfg;
  }

  /** Blocking; called once by InfraLifecycle (never on an event loop). */
  public synchronized void connect() {
    if (registry != null) {
      return;
    }
    if (resources == null) {
      resources = DefaultClientResources.builder().ioThreadPoolSize(2).computationThreadPoolSize(2).build();
    }
    RedisClusterAsyncCommands<String, String> reg = open(cfg.registryUri());
    RedisClusterAsyncCommands<String, String> st = cfg.stateUri().equals(cfg.registryUri()) ? reg : open(cfg.stateUri());
    state = st;
    registry = reg;
  }

  private RedisClusterAsyncCommands<String, String> open(String uri) {
    Duration timeout = cfg.commandTimeout();
    TimeoutOptions timeouts = TimeoutOptions.enabled(timeout);
    if (cfg.topology() == RedisTopology.CLUSTER) {
      RedisClusterClient client = RedisClusterClient.create(resources, RedisURI.create(uri));
      client.setOptions(ClusterClientOptions.builder()
          .timeoutOptions(timeouts)
          .requestQueueSize(REQUEST_QUEUE)
          .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
          .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
              .enablePeriodicRefresh(Duration.ofSeconds(30))
              .enableAllAdaptiveRefreshTriggers()
              .build())
          .build());
      var conn = client.connect();
      track(conn, client::shutdown);
      return conn.async();
    }
    RedisClient client = RedisClient.create(resources, RedisURI.create(uri));
    client.setOptions(ClientOptions.builder()
        .timeoutOptions(timeouts)
        .requestQueueSize(REQUEST_QUEUE)
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .build());
    try {
      var conn = client.connect();
      track(conn, client::shutdown);
      return conn.async();
    } catch (RuntimeException e) {
      client.shutdown();
      throw e;
    }
  }

  private void track(StatefulConnection<?, ?> conn, Runnable shutdown) {
    closeables.add(conn::close);
    closeables.add(shutdown::run);
  }

  public RedisClusterAsyncCommands<String, String> registry() {
    RedisClusterAsyncCommands<String, String> r = registry;
    if (r == null) {
      throw new IllegalStateException("Redis not connected yet");
    }
    return r;
  }

  public RedisClusterAsyncCommands<String, String> state() {
    RedisClusterAsyncCommands<String, String> s = state;
    if (s == null) {
      throw new IllegalStateException("Redis not connected yet");
    }
    return s;
  }

  public String registryUri() {
    return cfg.registryUri();
  }

  @Override
  public synchronized void close() {
    registry = null;
    state = null;
    for (AutoCloseable c : closeables) {
      try {
        c.close();
      } catch (Exception ignored) {
        // best effort on shutdown
      }
    }
    closeables.clear();
    if (resources != null) {
      resources.shutdown(0, 2, java.util.concurrent.TimeUnit.SECONDS);
      resources = null;
    }
  }
}
