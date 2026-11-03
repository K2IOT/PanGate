package com.pangate.starter.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PrincipalHasherTest {

  private static final byte[] KEY_A = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
  private static final byte[] KEY_B = "fedcba9876543210fedcba9876543210".getBytes(StandardCharsets.UTF_8);

  @Test
  void deterministicSixteenHexChars() {
    PrincipalHasher h = new PrincipalHasher(KEY_A);

    String first = h.hash("t1:u1");

    assertThat(first).matches("^[0-9a-f]{16}$");
    assertThat(h.hash("t1:u1")).isEqualTo(first);
    assertThat(h.hash("t1:u2")).isNotEqualTo(first);
  }

  @Test
  void differentKeysDifferentHashes() {
    assertThat(new PrincipalHasher(KEY_A).hash("t1:u1")).isNotEqualTo(new PrincipalHasher(KEY_B).hash("t1:u1"));
  }
}
