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

  @Test
  void directMemoryExplainsTheRiskAndGivesTheProdSetting() {
    FailureAnalysis a = new JvmConfigFailureAnalyzer().analyze(new JvmConfigException("MaxDirectMemorySize"));
    assertThat(a.getDescription()).startsWith("-XX:MaxDirectMemorySize is not set")
        .contains("heap plus direct memory");
    assertThat(a.getAction()).contains("profile prod", "25% of container memory",
        "JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize=2g");
  }
}
