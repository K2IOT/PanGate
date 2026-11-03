package com.pangate.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SdkVersionTest {

  @Test
  void comparesNumericSegments() {
    assertThat(SdkVersion.compare("1.10.0", "1.9.9")).isPositive();
    assertThat(SdkVersion.compare("2.0", "2.0.0")).isZero();
    assertThat(SdkVersion.compare("1.2.3-beta", "1.2.3")).isZero();
    assertThat(SdkVersion.compare("1.2.3+build7", "1.2.4")).isNegative();
    assertThat(SdkVersion.compare("x", "0.0.1")).isNegative();
    assertThat(SdkVersion.compare("", "0")).isZero();
    assertThat(SdkVersion.compare(null, "1")).isNegative();
    assertThat(SdkVersion.compare("99999999999999999999", "1")).isPositive();
  }
}
