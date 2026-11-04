package com.pangate.gateway.tenant;

import com.pangate.gateway.lifecycle.RedisConnections;
import com.pangate.gateway.registry.LuaScript;
import io.lettuce.core.ScriptOutputType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

/**
 * Cell-wide tenant connection totals on Redis state (spec §4.6 step i, plan ruling R2): hash {@code tenantconns:{t}},
 * field = node id, value {@code "<total>|<expiresAtMs>"}. Every node reports its total each 10 s with a 30 s expiry,
 * so the entry of a killed node stops counting by itself; a borrow compares the live total with maxConns atomically.
 */
public final class TenantCapBorrower implements TenantCap.Borrower {

  static final Duration TTL = Duration.ofSeconds(30);
  static final Duration TIMEOUT = Duration.ofMillis(200);

  private static final LuaScript BORROW = new LuaScript("""
      local now = tonumber(ARGV[4])
      local sum = tonumber(ARGV[2])
      local all = redis.call('HGETALL', KEYS[1])
      for i = 1, #all, 2 do
        if all[i] ~= ARGV[1] then
          local sep = string.find(all[i + 1], '|', 1, true)
          if sep and tonumber(string.sub(all[i + 1], sep + 1)) > now then
            sum = sum + tonumber(string.sub(all[i + 1], 1, sep - 1))
          end
        end
      end
      if sum > tonumber(ARGV[3]) then
        return 0
      end
      redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. '|' .. (now + tonumber(ARGV[5])))
      redis.call('PEXPIRE', KEYS[1], 2 * tonumber(ARGV[5]))
      return 1
      """, ScriptOutputType.INTEGER);

  private final RedisConnections redis;
  private final String nodeId;
  private final LongSupplier wallMillis;

  public TenantCapBorrower(RedisConnections redis, String nodeId, LongSupplier wallMillis) {
    this.redis = redis;
    this.nodeId = nodeId;
    this.wallMillis = wallMillis;
  }

  public static int quota(int maxConns, int nodes) {
    return (int) Math.ceil(maxConns * 1.1 / Math.max(1, nodes));
  }

  static String key(String tenant) {
    return "tenantconns:{" + tenant + "}";
  }

  @Override
  public CompletionStage<Boolean> borrow(String tenant, int maxConns, int ownTotalAfter) {
    return BORROW.<Long>run(redis.state(), key(tenant), nodeId, Integer.toString(ownTotalAfter),
            Integer.toString(maxConns), Long.toString(wallMillis.getAsLong()), Long.toString(TTL.toMillis()))
        .toCompletableFuture().orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
        .thenApply(r -> r != null && r == 1L);
  }

  /** Writes this node's totals for every tenant with a cap; a total of 0 removes the field. */
  public CompletionStage<Void> report(Map<String, Integer> totals, ToIntFunction<String> maxConns) {
    List<CompletableFuture<?>> writes = new ArrayList<>();
    long expires = wallMillis.getAsLong() + TTL.toMillis();
    totals.forEach((tenant, total) -> {
      if (maxConns.applyAsInt(tenant) <= 0) {
        return;
      }
      String k = key(tenant);
      if (total <= 0) {
        writes.add(redis.state().hdel(k, nodeId).toCompletableFuture());
      } else {
        writes.add(redis.state().hset(k, nodeId, total + "|" + expires).toCompletableFuture());
        writes.add(redis.state().pexpire(k, 2 * TTL.toMillis()).toCompletableFuture());
      }
    });
    return CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new));
  }
}
