package com.pangate.gateway.registry;

import com.pangate.gateway.lifecycle.RedisConnections;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionListener;
import com.pangate.gateway.session.SessionState;
import io.lettuce.core.ScriptOutputType;
import io.micrometer.core.instrument.Counter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Writes {@code reg:{principal}} (spec §6.2): field = connId, value = {@code nodeId|epoch|connectedAtMs|platform}.
 * Fire-and-forget from the event loop; failures go to the {@link RetryQueue}. Stale entries are swept by the push
 * router, not the gateway.
 */
public final class RegistryWriter implements SessionListener {

  private static final LuaScript REGISTER = new LuaScript("""
      local prev = {}
      if ARGV[3] == '1' then prev = redis.call('HGETALL', KEYS[1]); redis.call('DEL', KEYS[1]) end
      redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
      return prev
      """, ScriptOutputType.MULTI);

  private final RedisConnections redis;
  private final NodeState node;
  private final RetryQueue retry;
  private final Counter redisErrors;

  public RegistryWriter(RedisConnections redis, NodeState node, RetryQueue retry, GatewayMetrics metrics) {
    this.redis = redis;
    this.node = node;
    this.retry = retry;
    this.redisErrors = metrics.counter("gw.registry.write.errors", "reason", "redis");
  }

  static String value(String nodeId, long epoch, long connectedAtMs, String platform) {
    return nodeId + "|" + epoch + "|" + connectedAtMs + "|" + (platform == null ? "" : platform);
  }

  /** Returns the entries replaced in single-session mode (empty otherwise). Failures are retried. */
  public CompletionStage<Map<String, String>> register(Session s, boolean single) {
    CompletionStage<Map<String, String>> st = registerOnce(s, single);
    st.whenComplete((v, e) -> {
      if (e != null) {
        redisErrors.increment();
        retry.retryLater("register", () -> registerOnce(s, single), () -> s.state() == SessionState.ACTIVE || s.state() == SessionState.DRAINING);
      }
    });
    return st;
  }

  private CompletionStage<Map<String, String>> registerOnce(Session s, boolean single) {
    String principal = s.identity().principalId();
    try {
      return REGISTER.<List<Object>>run(redis.registry(), RegistryKeys.reg(principal), s.connId(),
          value(node.nodeId(), node.epoch(), s.connectedAtMs(), s.identity().platform()), single ? "1" : "0")
          .thenApply(RegistryWriter::toMap);
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private static Map<String, String> toMap(List<Object> flat) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; flat != null && i + 1 < flat.size(); i += 2) {
      m.put(str(flat.get(i)), str(flat.get(i + 1)));
    }
    return m;
  }

  private static String str(Object o) {
    return o instanceof byte[] b ? new String(b, java.nio.charset.StandardCharsets.UTF_8) : String.valueOf(o);
  }

  public void unregister(String principal, String connId) {
    unregisterOnce(principal, connId).whenComplete((v, e) -> {
      if (e != null) {
        redisErrors.increment();
        retry.retryLater("unregister", () -> unregisterOnce(principal, connId), () -> true);
      }
    });
  }

  private CompletionStage<Long> unregisterOnce(String principal, String connId) {
    try {
      return redis.registry().hdel(RegistryKeys.reg(principal), connId);
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public void activated(Session s) {
    register(s, false);
  }

  @Override
  public void closed(Session s) {
    if (s.connId() != null) {
      unregister(s.identity().principalId(), s.connId());
    }
  }
}
