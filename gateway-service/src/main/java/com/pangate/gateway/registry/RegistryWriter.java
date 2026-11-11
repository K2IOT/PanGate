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
 * Writes {@code reg:{principal}} (spec §6.2): field = connId, value v2 = {@code nodeId|epoch|connectedAtMs|platform|device}
 * (push contracts spec §4: device last, may contain '|'; readers split with a limit of 5 and accept 4-part v1).
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
  private final com.pangate.gateway.control.TenantLimitsStore tenants;
  private final java.util.function.BiConsumer<Session, Map<String, String>> onReplaced;

  /**
   * @param onReplaced single-session mode: called on the session's loop with the entries a successful register
   *     replaced (its own connId removed), when there are any (spec §6.6)
   */
  public RegistryWriter(RedisConnections redis, NodeState node, RetryQueue retry, GatewayMetrics metrics,
      com.pangate.gateway.control.TenantLimitsStore tenants,
      java.util.function.BiConsumer<Session, Map<String, String>> onReplaced) {
    this.redis = redis;
    this.node = node;
    this.retry = retry;
    this.tenants = tenants;
    this.onReplaced = onReplaced;
    this.redisErrors = metrics.counter("gw.registry.write.errors", "reason", "redis");
  }

  static String value(String nodeId, long epoch, long connectedAtMs, String platform, String device) {
    // platform comes from the client's HELLO: a '|' in it would shift the device (push contracts spec §4, §5)
    return nodeId + "|" + epoch + "|" + connectedAtMs + "|" + (platform == null ? "" : platform.replace('|', '_'))
        + "|" + (device == null ? "" : device);
  }

  /**
   * Returns the entries replaced in single-session mode (empty otherwise). Called on the session's loop; failures
   * are retried, each retry again on the loop so it is ordered with close (no entry re-created after HDEL).
   */
  public CompletionStage<Map<String, String>> register(Session s, boolean single) {
    CompletableFuture<Map<String, String>> st = send(s, single);
    st.whenComplete((v, e) -> {
      if (e != null) {
        redisErrors.increment();
        retry.retryLater("register", () -> retryOnLoop(s, single), () -> wanted(s));
      }
    });
    return st;
  }

  private static boolean wanted(Session s) {
    return s.state() == SessionState.ACTIVE || s.state() == SessionState.DRAINING;
  }

  /** On the owning loop: dispatches the script and remembers it so close can HDEL again after it lands. */
  private CompletableFuture<Map<String, String>> send(Session s, boolean single) {
    CompletableFuture<Map<String, String>> cf = registerOnce(s, single).toCompletableFuture();
    s.registryWrite(cf);
    if (single) {
      cf.thenAccept(prev -> replaced(s, prev));
    }
    // Forget it once done (on the loop): an idle session must not keep the future and its result.
    cf.whenComplete((v, e) -> s.loop().eventLoop().execute(() -> {
      if (s.registryWrite() == cf) {
        s.registryWrite(null);
      }
    }));
    return cf;
  }

  /**
   * Registers can reach Redis out of connection order (retries, jitter between nodes). An entry newer than this
   * session must win: it is written back and this session is the one kicked (spec §6.6). Older entries go to
   * {@code onReplaced}.
   */
  private void replaced(Session s, Map<String, String> prev) {
    Map<String, String> older = new HashMap<>();
    Map<String, String> newer = new HashMap<>();
    prev.forEach((connId, value) -> {
      if (!connId.equals(s.connId())) {
        long at = connectedAtMs(value);
        boolean isNewer = at >= 0
            && com.pangate.gateway.downstream.ControlPushHandler.older(s.connectedAtMs(), s.connId(), at, connId);
        (isNewer ? newer : older).put(connId, value);
      }
    });
    if (!newer.isEmpty()) {
      String key = RegistryKeys.reg(s.identity().principalId());
      try {
        redis.registry().hset(key, newer).whenComplete((v, e) -> {
          if (e != null) {
            redisErrors.increment();
          }
        });
      } catch (RuntimeException e) {
        redisErrors.increment();
      }
      s.loop().eventLoop().execute(() -> {
        if (wanted(s)) {
          s.close(com.pangate.gateway.protocol.CloseCode.DUPLICATE_SESSION, "KICKED");
        }
      });
    }
    if (!older.isEmpty()) {
      s.loop().eventLoop().execute(() -> onReplaced.accept(s, older));
    }
  }

  /** v1 or v2 value → connectedAtMs (third part), or -1 if malformed. */
  static long connectedAtMs(String value) {
    String[] parts = value.split("\\|", -1);
    try {
      return parts.length >= 3 ? Long.parseLong(parts[2]) : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private CompletionStage<?> retryOnLoop(Session s, boolean single) {
    CompletableFuture<Object> done = new CompletableFuture<>();
    s.loop().eventLoop().execute(() -> {
      if (!wanted(s)) {
        done.complete(null);
        return;
      }
      send(s, single).whenComplete((v, e) -> {
        if (e != null) {
          done.completeExceptionally(e);
        } else {
          done.complete(null);
        }
      });
    });
    return done;
  }

  private CompletionStage<Map<String, String>> registerOnce(Session s, boolean single) {
    String principal = s.identity().principalId();
    try {
      return REGISTER.<List<Object>>run(redis.registry(), RegistryKeys.reg(principal), s.connId(),
          value(node.nodeId(), node.epoch(), s.connectedAtMs(), s.identity().platform(),
              s.identity().deviceId()), single ? "1" : "0")
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
    register(s, tenants.policy(s.identity().tenantId()).singleSession());
  }

  /**
   * HDEL now, and once more after an in-flight register lands: a register can reach Redis after this HDEL (its
   * EVALSHA→EVAL fallback after a Redis restart is sent from a Lettuce thread) and would otherwise leave a stale
   * entry for a closed connection.
   */
  @Override
  public void closed(Session s) {
    if (s.connId() == null) {
      return;
    }
    String principal = s.identity().principalId();
    String connId = s.connId();
    unregister(principal, connId);
    CompletableFuture<?> inFlight = s.registryWrite();
    if (inFlight != null && !inFlight.isDone()) {
      inFlight.whenComplete((v, e) -> unregister(principal, connId));
    }
  }
}
