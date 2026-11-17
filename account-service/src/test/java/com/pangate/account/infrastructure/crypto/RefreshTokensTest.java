package com.pangate.account.infrastructure.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RefreshTokensTest {

  @Test
  void tokensAreRandomAndHashStable() {
    String a = RefreshTokens.newToken();
    String b = RefreshTokens.newToken();
    assertThat(a).startsWith("rt_").hasSize(46).isNotEqualTo(b);
    assertThat(RefreshTokens.looksValid(a)).isTrue();
    assertThat(RefreshTokens.looksValid("rt_short")).isFalse();
    assertThat(RefreshTokens.looksValid("xx" + a.substring(2))).isFalse();
    assertThat(RefreshTokens.hash(a)).hasSize(32).isEqualTo(RefreshTokens.hash(a));
  }
}
