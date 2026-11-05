package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class LoadShedLadderTest {

  private final AtomicLong ms = new AtomicLong(1_000_000);
  private final LoadShedLadder ladder = new LoadShedLadder(ms::get);

  private static LoadShedLadder.Signals s(double budget, double lag, double cpu, double kafka, boolean rising,
      double direct) {
    return new LoadShedLadder.Signals(budget, lag, cpu, kafka, rising, direct);
  }

  private static final LoadShedLadder.Signals QUIET = s(0, 0, 0, 0, false, 0);

  /** Evaluates every 100 ms for {@code millis}, returning the last level. */
  private int hold(LoadShedLadder.Signals sig, long millis) {
    int level = ladder.evaluate(sig);
    for (long t = 100; t <= millis; t += 100) {
      ms.addAndGet(100);
      level = ladder.evaluate(sig);
    }
    return level;
  }

  @Test
  void quietSignalsAreLevel0() {
    assertThat(ladder.evaluate(QUIET)).isZero();
  }

  @Test
  void eachEntryConditionSelectsItsLevel() {
    assertThat(new LoadShedLadder(ms::get).evaluate(s(.71, 0, 0, 0, false, 0))).isEqualTo(1);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(0, .051, 0, 0, false, 0))).isEqualTo(1);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(0, 0, .71, 0, false, 0))).isEqualTo(1);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(0, 0, 0, .71, false, 0))).isEqualTo(2);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(0, 0, .81, 0, false, 0))).isEqualTo(3);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(0, 0, 0, 0, true, 0))).isEqualTo(3);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(.91, 0, 0, 0, false, 0))).isEqualTo(4);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(1.0, 0, 0, 0, false, 0))).isEqualTo(5);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(0, 0, 0, 0, false, .86))).isEqualTo(5);
    assertThat(new LoadShedLadder(ms::get).evaluate(s(.70, .05, .70, .70, false, .85))).isZero();
  }

  @Test
  void highestActiveLevelWins() {
    assertThat(ladder.evaluate(s(.95, 0, 0, .8, false, 0))).isEqualTo(4);
  }

  @Test
  void level1ExitNeedsThirtySecondsOfCalm() {
    assertThat(ladder.evaluate(s(.75, 0, 0, 0, false, 0))).isEqualTo(1);
    LoadShedLadder.Signals calm = s(.5, 0, .1, 0, false, 0);
    assertThat(hold(calm, 29_900)).isEqualTo(1);
    ms.addAndGet(100);
    assertThat(ladder.evaluate(calm)).isZero();
  }

  @Test
  void betweenExitAndEntryThresholdsTheLevelHolds() {
    ladder.evaluate(s(.75, 0, 0, 0, false, 0));
    assertThat(hold(s(.65, 0, 0, 0, false, 0), 60_000)).isEqualTo(1);
  }

  @Test
  void spikeDuringCalmRestartsTheClock() {
    ladder.evaluate(s(.75, 0, 0, 0, false, 0));
    LoadShedLadder.Signals calm = s(.5, 0, .1, 0, false, 0);
    hold(calm, 20_000);
    ms.addAndGet(100);
    ladder.evaluate(s(.65, 0, 0, 0, false, 0));  // not below .60: calm is broken
    ms.addAndGet(100);
    assertThat(hold(calm, 29_000)).isEqualTo(1);
    assertThat(hold(calm, 1_100)).isZero();
  }

  @Test
  void level2And4ExitImmediatelyBelowTheirExitThreshold() {
    assertThat(ladder.evaluate(s(0, 0, 0, .75, false, 0))).isEqualTo(2);
    assertThat(ladder.evaluate(s(0, 0, 0, .6, false, 0))).isEqualTo(2);
    assertThat(ladder.evaluate(s(0, 0, 0, .49, false, 0))).isZero();

    LoadShedLadder l = new LoadShedLadder(ms::get);
    assertThat(l.evaluate(s(.95, 0, 0, 0, false, 0))).isEqualTo(4);
    assertThat(l.evaluate(s(.85, 0, 0, 0, false, 0))).isEqualTo(4);
    assertThat(l.evaluate(s(.79, 0, 0, 0, false, 0))).isEqualTo(1);  // L1 (entered at .95) still waits for calm
  }

  @Test
  void level3ExitNeedsSixtySecondsOfCpuBelow70() {
    assertThat(ladder.evaluate(s(0, 0, .85, 0, false, 0))).isEqualTo(3);
    LoadShedLadder.Signals cpu65 = s(0, 0, .65, 0, false, 0);
    assertThat(hold(cpu65, 59_900)).isEqualTo(3);
    ms.addAndGet(100);
    assertThat(ladder.evaluate(cpu65)).isEqualTo(1);  // L1 (cpu .85 > .70) still needs cpu < .60 for 30 s
  }

  @Test
  void level5DropsAsSoonAsConditionClears() {
    assertThat(ladder.evaluate(s(0, 0, 0, 0, false, .9))).isEqualTo(5);
    assertThat(ladder.evaluate(QUIET)).isZero();
  }
}
