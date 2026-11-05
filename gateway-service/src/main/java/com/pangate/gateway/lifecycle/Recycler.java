package com.pangate.gateway.lifecycle;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import io.micrometer.core.instrument.Counter;
import java.util.random.RandomGenerator;

/**
 * Connection recycling (spec §5.9): each session is asked to reconnect after U[min, max], spreading reconnects over
 * time. Postponed by U[60, 300] s while shedding (level ≥ 3) or when the tenant switched {@code recycle.enabled} off.
 * Used on the owning loop only; the random generator is per instance (one per loop).
 */
public final class Recycler {

  private final GatewayProperties.Recycling cfg;
  private final NodeState node;
  private final TenantLimitsStore tenants;
  private final RandomGenerator rnd;
  private final Counter reconnects;

  public Recycler(GatewayProperties.Recycling cfg, NodeState node, TenantLimitsStore tenants, RandomGenerator rnd,
      GatewayMetrics m) {
    this.cfg = cfg;
    this.node = node;
    this.tenants = tenants;
    this.rnd = rnd;
    this.reconnects = m.counter("gw.recycle.reconnects");
  }

  public long firstRecycleAtSec(long nowSec) {
    long min = cfg.min().toSeconds();
    long max = Math.max(min, cfg.max().toSeconds());
    return nowSec + min + (max > min ? rnd.nextLong(max - min + 1) : 0);
  }

  /** On the loop, when the session's recycle time has come. Returns its next recycle time. */
  public long onDue(Session s, long nowSec) {
    if (node.shedLevel() < 3 && tenants.policy(s.identity().tenantId()).recycleEnabled()) {
      reconnects.increment();
      s.drain(rnd.nextLong(5_001), false, cfg.grace().toSeconds());
      return Long.MAX_VALUE;
    }
    return nowSec + 60 + rnd.nextLong(241);
  }
}
