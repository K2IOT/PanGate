package com.pangate.gateway.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JvmSettingsTest {

  private static JvmSettings noDirect() {
    return new JvmSettings(List.of("G1"), 1L << 30, 1L << 30, false, "disabled");
  }

  @Test
  void prodWithoutDirectFails() {
    assertThatThrownBy(() -> noDirect().check(Set.of("prod"))).isInstanceOf(JvmConfigException.class);
  }

  @Test
  void devWithoutDirectPasses() {
    assertThatCode(() -> noDirect().check(Set.of("dev"))).doesNotThrowAnyException();
  }

  @Test
  void prodWithDirectPasses() {
    assertThatCode(() -> new JvmSettings(List.of("ZGC"), 1L << 30, 1L << 28, true, "disabled").check(Set.of("prod")))
        .doesNotThrowAnyException();
  }

  @Test
  void describeFormat() {
    assertThat(noDirect().describe()).startsWith("jvm gc=G1 heapMax=1024MiB directMax=default");
    assertThat(new JvmSettings(List.of("ZGC Minor Cycles", "ZGC Major Cycles"), 1L << 30, 1L << 28, true, "simple")
        .describe()).isEqualTo("jvm gc=ZGC Minor Cycles,ZGC Major Cycles heapMax=1024MiB directMax=256MiB leak=simple");
  }

  @Test
  void readsThisJvm() {
    JvmSettings s = JvmSettings.read();
    assertThat(s.gcNames()).isNotEmpty();
    assertThat(s.maxHeapBytes()).isPositive();
  }
}
