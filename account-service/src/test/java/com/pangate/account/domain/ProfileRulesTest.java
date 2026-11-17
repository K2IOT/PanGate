package com.pangate.account.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProfileRulesTest {

  static String code(Runnable r) {
    try {
      r.run();
      return "ok";
    } catch (DomainException e) {
      return e.code();
    }
  }

  @Test
  void displayNameAndBio() {
    assertThat(code(() -> ProfileRules.displayName("  "))).isEqualTo("invalid_display_name");
    assertThat(ProfileRules.displayName("  Lan  ")).isEqualTo("Lan");
    assertThat(code(() -> ProfileRules.displayName("x".repeat(64)))).isEqualTo("ok");
    assertThat(code(() -> ProfileRules.displayName("x".repeat(65)))).isEqualTo("invalid_display_name");
    assertThat(code(() -> ProfileRules.displayName("La\u0007n"))).isEqualTo("invalid_display_name");
    assertThat(code(() -> ProfileRules.bio("x".repeat(140)))).isEqualTo("ok");
    assertThat(code(() -> ProfileRules.bio("x".repeat(141)))).isEqualTo("invalid_bio");
    assertThat(ProfileRules.bio(null)).isNull();
  }
}
