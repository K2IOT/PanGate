package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HandshakeTrendTest {

  private final HandshakeTrend trend = new HandshakeTrend();

  @Test
  void strictlyRisingForSixSecondSamplesIsRising() {
    for (int sec = 0; sec <= 5; sec++) {
      for (int tick = 0; tick < 10; tick++) {
        trend.record(sec * 1000L + tick * 100, 10 + sec);
      }
    }
    assertThat(trend.rising()).isTrue();
  }

  @Test
  void fewerThanSixSamplesIsNotRising() {
    for (int sec = 0; sec <= 4; sec++) {
      trend.record(sec * 1000L, 10 + sec);
    }
    assertThat(trend.rising()).isFalse();
  }

  @Test
  void aFlatOrFallingSecondBreaksTheTrend() {
    int[] values = {1, 2, 3, 3, 4, 5};
    for (int sec = 0; sec < values.length; sec++) {
      trend.record(sec * 1000L, values[sec]);
    }
    assertThat(trend.rising()).isFalse();
    trend.record(6000, 6);
    trend.record(7000, 7);
    assertThat(trend.rising()).isFalse();  // window 3,4,5,6,7 + the flat 3 → still broken
    trend.record(8000, 8);
    assertThat(trend.rising()).isTrue();   // 3,4,5,6,7,8
  }
}
