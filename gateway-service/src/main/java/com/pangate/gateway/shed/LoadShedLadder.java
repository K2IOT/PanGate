package com.pangate.gateway.shed;

import java.util.function.LongSupplier;

/**
 * Load-shedding ladder (spec §7.3), evaluated every 100 ms on gw-tick. Each level has its own entry condition and
 * exit condition; L1 and L3 leave only after their exit condition held continuously (30 s, 60 s). The current level
 * is the highest active one. Level 6 (operator kill switch) is not modelled: control.proto has no switch for it.
 */
public final class LoadShedLadder {

  public record Signals(double budgetRatio, double lagP99Sec, double cpu, double kafkaBufferRatio,
      boolean handshakesRising, double directMemoryRatio) {}

  /** One level: active flag plus, for hysteresis, since when the exit condition has held. */
  private static final class Level {
    final long calmMillis;
    boolean active;
    long calmSince = -1;

    Level(long calmMillis) {
      this.calmMillis = calmMillis;
    }

    void update(boolean enter, boolean exit, long now) {
      if (enter) {
        active = true;
        calmSince = -1;
      } else if (active && exit) {
        if (calmSince < 0) {
          calmSince = now;
        }
        if (now - calmSince >= calmMillis) {
          active = false;
          calmSince = -1;
        }
      } else {
        calmSince = -1;
      }
    }
  }

  private final LongSupplier monoMillis;
  private final Level l1 = new Level(30_000);
  private final Level l2 = new Level(0);
  private final Level l3 = new Level(60_000);
  private final Level l4 = new Level(0);
  private final Level l5 = new Level(0);

  public LoadShedLadder(LongSupplier monoMillis) {
    this.monoMillis = monoMillis;
  }

  public int evaluate(Signals s) {
    long now = monoMillis.getAsLong();
    l1.update(s.budgetRatio() > .70 || s.lagP99Sec() > .050 || s.cpu() > .70,
        s.budgetRatio() < .60 && s.lagP99Sec() < .030 && s.cpu() < .60, now);
    l2.update(s.kafkaBufferRatio() > .70, s.kafkaBufferRatio() < .50, now);
    l3.update(s.cpu() > .80 || s.handshakesRising(), s.cpu() < .70, now);
    l4.update(s.budgetRatio() > .90, s.budgetRatio() < .80, now);
    boolean five = s.budgetRatio() >= 1.0 || s.directMemoryRatio() > .85;
    l5.update(five, !five, now);
    return l5.active ? 5 : l4.active ? 4 : l3.active ? 3 : l2.active ? 2 : l1.active ? 1 : 0;
  }
}
