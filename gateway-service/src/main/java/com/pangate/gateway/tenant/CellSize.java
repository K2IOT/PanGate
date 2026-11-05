package com.pangate.gateway.tenant;

import com.pangate.gateway.lifecycle.RedisConnections;
import com.pangate.gateway.registry.RegistryKeys;
import io.lettuce.core.Range;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Live nodes of the cell for the tenant quota: ZCOUNT nodes:live (now − 30 s) +inf, at least 1. */
public final class CellSize implements IntSupplier {

  private static final Logger log = LoggerFactory.getLogger(CellSize.class);

  private final RedisConnections redis;
  private final LongSupplier wallMillis;
  private volatile int nodes = 1;

  public CellSize(RedisConnections redis, LongSupplier wallMillis) {
    this.redis = redis;
    this.wallMillis = wallMillis;
  }

  @Override
  public int getAsInt() {
    return nodes;
  }

  /** Blocking (≤ 1 s): offload executor. A failure keeps the last value. */
  public void refresh() {
    try {
      Long n = redis.registry().zcount(RegistryKeys.NODES_LIVE,
          Range.create((double) (wallMillis.getAsLong() - 30_000), Double.POSITIVE_INFINITY))
          .get(1, TimeUnit.SECONDS);
      nodes = (int) Math.max(1, n == null ? 1 : n);
    } catch (Exception e) {
      log.debug("cell size refresh failed, keeping {}: {}", nodes, e.toString());
    }
  }
}
