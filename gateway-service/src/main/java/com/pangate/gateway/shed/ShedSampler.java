package com.pangate.gateway.shed;

import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

/**
 * gw-tick, every 100 ms (spec §7.3): gathers the ladder's signals, publishes the level to {@link NodeState} (read by
 * the pacer, push stream, upstream, admission, upgrade guard and recycler) and runs the level-5 relief.
 */
public final class ShedSampler {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ShedSampler.class);

  private final LoadShedLadder ladder;
  private final DoubleSupplier budgetRatio;
  private final DoubleSupplier lagP99;
  private final DoubleSupplier cpu;
  private final DoubleSupplier kafkaRatio;
  private final DoubleSupplier directRatio;
  private final LoopRegistry loops;
  private final HandshakeTrend trend;
  private final NodeState node;
  private final Runnable relieve;
  private final LongSupplier monoMillis;
  private long lastLogMs = Long.MIN_VALUE / 2;
  private int suppressed;

  public ShedSampler(LoadShedLadder ladder, DoubleSupplier budgetRatio, DoubleSupplier lagP99, DoubleSupplier cpu,
      DoubleSupplier kafkaRatio, DoubleSupplier directRatio, LoopRegistry loops, HandshakeTrend trend,
      NodeState node, Runnable relieve, LongSupplier monoMillis, GatewayMetrics m) {
    this.ladder = ladder;
    this.budgetRatio = budgetRatio;
    this.lagP99 = lagP99;
    this.cpu = cpu;
    this.kafkaRatio = kafkaRatio;
    this.directRatio = directRatio;
    this.loops = loops;
    this.trend = trend;
    this.node = node;
    this.relieve = relieve;
    this.monoMillis = monoMillis;
    m.gauge("gw.shed.level", node, NodeState::shedLevel);
  }

  public void sample() {
    long now = monoMillis.getAsLong();
    int handshaking = 0;
    for (LoopContext c : loops.all()) {
      handshaking += c.handshaking();
    }
    trend.record(now, handshaking);
    // Until warm-up is done, CPU (Spring start, JIT, warm-up TLS) and the warm-up handshake ramp are start-up noise:
    // counting them could hold a just-ready node at L3, 503-ing every upgrade for 60 s. Memory signals always count.
    boolean warm = node.warmedUp();
    LoadShedLadder.Signals s = new LoadShedLadder.Signals(budgetRatio.getAsDouble(), lagP99.getAsDouble(),
        warm ? Math.max(0, cpu.getAsDouble()) : 0, kafkaRatio.getAsDouble(), warm && trend.rising(),
        directRatio.getAsDouble());
    int level = ladder.evaluate(s);
    int before = node.shedLevel();
    if (level != before) {
      // L4/L5 have no time hysteresis (spec §7.3): near budget 1.0 they can flip every sample. Log at most once a
      // second; the gw.shed.level gauge still shows every change.
      if (now - lastLogMs >= 1000) {
        log.info("shed level {} -> {} ({}){}", before, level, s,
            suppressed > 0 ? " [" + suppressed + " changes suppressed in the last second]" : "");
        lastLogMs = now;
        suppressed = 0;
      } else {
        suppressed++;
      }
      node.shedLevel(level);
    }
    if (level == 5) {
      relieve.run();
    }
  }
}
