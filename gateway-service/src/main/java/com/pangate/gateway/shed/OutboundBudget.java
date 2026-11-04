package com.pangate.gateway.shed;

import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import io.netty.util.internal.PlatformDependent;

/**
 * Outbound budget (spec §7.2): bytes queued in Netty outbound buffers across the node, sampled every 100 ms from each
 * loop's dirty sessions. The effective budget is min(configured, 35% of max direct memory), so a small dev JVM does
 * not sit at shed level 5.
 */
public final class OutboundBudget {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(OutboundBudget.class);

  private final LoopRegistry loops;
  private final long budget;
  private volatile long lastTotal;

  public OutboundBudget(long configuredBytes, long maxDirectMemory, LoopRegistry loops, GatewayMetrics m) {
    this.loops = loops;
    this.budget = effective(configuredBytes, maxDirectMemory);
    if (budget < configuredBytes) {
      log.info("outbound budget lowered from {} to {} bytes (35% of max direct memory {})", configuredBytes, budget,
          maxDirectMemory);
    }
    m.gauge("gw.outbound.budget.ratio", this, OutboundBudget::ratio);
    m.gauge("gw.pending.outbound.bytes", this, b -> b.lastTotal);
    m.gauge("gw.direct.memory.bytes", this, b -> PlatformDependent.usedDirectMemory());
  }

  static long effective(long configured, long maxDirect) {
    return Math.min(configured, (long) (0.35 * maxDirect));
  }

  public long budgetBytes() {
    return budget;
  }

  public long shareBytes() {
    return budget / loops.size();
  }

  /** gw-tick: records the totals the loops reported last round, then asks each loop for a fresh sample. */
  public void sample() {
    long total = 0;
    for (LoopContext c : loops.all()) {
      total += c.outboundBytes();
    }
    lastTotal = total;
    for (LoopContext c : loops.all()) {
      c.eventLoop().execute(c::sampleOutbound);
    }
  }

  public double ratio() {
    return lastTotal / (double) budget;
  }
}
