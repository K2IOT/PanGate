package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

class CpuWindowTest {

  @Test
  void averagesTheLastHundredSamples() {
    CpuWindow w = new CpuWindow(100);
    assertThat(w.average()).isZero();
    w.record(1.0);  // one 100 ms spike
    assertThat(w.average()).isCloseTo(1.0, within(1e-9));
    for (int i = 0; i < 99; i++) {
      w.record(0.5);
    }
    assertThat(w.average()).isCloseTo(0.505, within(1e-9));
    w.record(0.5);  // the spike leaves the window
    assertThat(w.average()).isCloseTo(0.5, within(1e-9));
  }

  @Test
  void negativeReadingsCountAsZero() {
    CpuWindow w = new CpuWindow(10);
    w.record(-1);
    w.record(0.4);
    assertThat(w.average()).isCloseTo(0.2, within(1e-9));
  }
}
