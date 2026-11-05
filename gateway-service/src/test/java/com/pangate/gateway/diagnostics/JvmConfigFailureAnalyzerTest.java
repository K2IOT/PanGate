package com.pangate.gateway.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.diagnostics.FailureAnalysis;

class JvmConfigFailureAnalyzerTest {

  @Test
  void namesTheMissingFlag() {
    FailureAnalysis a = new JvmConfigFailureAnalyzer().analyze(new JvmConfigException("SomeFlag"));
    assertThat(a.getDescription()).contains("-XX:SomeFlag is not set");
    assertThat(a.getAction()).contains("-XX:SomeFlag");
  }
}
