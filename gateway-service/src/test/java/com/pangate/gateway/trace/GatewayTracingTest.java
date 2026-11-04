package com.pangate.gateway.trace;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GatewayTracingTest {

  @Test
  void sampledFlagParsing() {
    assertThat(GatewayTracing.sampled("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")).isTrue();
    assertThat(GatewayTracing.sampled("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")).isFalse();
    assertThat(GatewayTracing.sampled("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03")).isTrue();
    assertThat(GatewayTracing.sampled("garbage")).isFalse();
    assertThat(GatewayTracing.sampled("00-00000000000000000000000000000000-00f067aa0ba902b7-01")).isFalse();
    assertThat(GatewayTracing.sampled(null)).isFalse();
  }
}
