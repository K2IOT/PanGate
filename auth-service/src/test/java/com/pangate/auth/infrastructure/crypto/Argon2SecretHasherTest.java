package com.pangate.auth.infrastructure.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class Argon2SecretHasherTest {

  private final Argon2SecretHasher hasher = new Argon2SecretHasher();

  @Test
  void hashMatchesOnlyTheSameSecret() {
    String h = hasher.hash("s3cret".toCharArray());
    assertThat(hasher.matches("s3cret".toCharArray(), h)).isTrue();
    assertThat(hasher.matches("s3cret!".toCharArray(), h)).isFalse();
    assertThat(hasher.matches("s3cret".toCharArray(), null)).isFalse();
    assertThat(hasher.hash("s3cret".toCharArray())).isNotEqualTo(h);  // random salt
  }

  @Test
  void hashEncodesArgon2id() {
    assertThat(hasher.hash("x".toCharArray())).startsWith("$argon2id$").contains("m=19456,t=2,p=1");
  }

  @Test
  void burnRunsWithoutError() {
    hasher.burn();
  }
}
