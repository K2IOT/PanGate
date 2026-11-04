package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.pangate.gateway.config.GatewayProperties;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AdmissionControllerTest {

  private final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1000));
  private final GatewayProperties.Admission cfg = new GatewayProperties.Admission(5000, 10000, 500, Duration.ofMinutes(2));

  @Test
  void rateIsRampFromBeforeReady() {
    AdmissionController a = new AdmissionController(cfg, 4, clock::get);

    assertThat(a.effectiveRate()).isEqualTo(500.0);
  }

  @Test
  void rampIsLinearFrom500To5000Over2Min() {
    AdmissionController a = new AdmissionController(cfg, 4, clock::get);
    a.markReady();

    assertThat(a.effectiveRate()).isCloseTo(500.0, within(1e-6));
    clock.addAndGet(TimeUnit.SECONDS.toNanos(60));
    assertThat(a.effectiveRate()).isCloseTo(2750.0, within(1e-6));
    clock.addAndGet(TimeUnit.SECONDS.toNanos(60));
    assertThat(a.effectiveRate()).isCloseTo(5000.0, within(1e-6));
    clock.addAndGet(TimeUnit.SECONDS.toNanos(600));
    assertThat(a.effectiveRate()).isCloseTo(5000.0, within(1e-6));
  }

  @Test
  void perLoopShareUsesRateDividedByLoops() {
    AdmissionController a = new AdmissionController(cfg, 4, clock::get);
    a.markReady();
    clock.addAndGet(TimeUnit.MINUTES.toNanos(3));

    int admitted = 0;
    while (a.tryAdmit(0)) {
      admitted++;
    }

    assertThat(admitted).isEqualTo(2500);
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    assertThat(a.tryAdmit(0)).isTrue();
    assertThat(a.tryAdmit(1)).isTrue();
  }

  @Test
  void overrideBaseRateApplies() {
    AdmissionController a = new AdmissionController(cfg, 4, clock::get);
    a.markReady();
    clock.addAndGet(TimeUnit.MINUTES.toNanos(3));

    a.applyKillSwitch(2000);

    assertThat(a.effectiveRate()).isCloseTo(2000.0, within(1e-6));
  }

  @Test
  void shedLevel3HalvesTheRate() {
    java.util.concurrent.atomic.AtomicInteger shed = new java.util.concurrent.atomic.AtomicInteger();
    AdmissionController a = new AdmissionController(cfg, 4, clock::get, shed::get);
    a.markReady();
    clock.addAndGet(TimeUnit.SECONDS.toNanos(600));
    shed.set(2);
    assertThat(a.effectiveRate()).isCloseTo(5000.0, within(1e-6));
    shed.set(3);
    assertThat(a.effectiveRate()).isCloseTo(2500.0, within(1e-6));
  }

  @Test
  void killSwitchReplacesAndZeroRestoresBase() {
    AdmissionController a = new AdmissionController(cfg, 4, clock::get, () -> 0);
    a.markReady();
    clock.addAndGet(TimeUnit.SECONDS.toNanos(600));
    a.applyKillSwitch(800);
    assertThat(a.effectiveRate()).isCloseTo(800.0, within(1e-6));
    a.applyKillSwitch(0);
    assertThat(a.effectiveRate()).isCloseTo(5000.0, within(1e-6));
  }
}
