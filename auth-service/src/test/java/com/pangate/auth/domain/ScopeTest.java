package com.pangate.auth.domain;

import static com.pangate.auth.domain.ClaimLimitsTest.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ScopeTest {

  @Test
  void scopeParseRejectsUnknownAndFormatsSorted() {
    assertCode(() -> Scope.parse("push:send bogus"), "invalid_scope");
    assertThat(Scope.parse("push:send  tokens:mint")).containsExactlyInAnyOrder(Scope.PUSH_SEND, Scope.TOKENS_MINT);
    assertThat(Scope.parse("")).isEmpty();
    assertThat(Scope.parse(null)).isEmpty();
    assertThat(Scope.format(Set.of(Scope.PUSH_SEND, Scope.TOKENS_MINT))).isEqualTo("tokens:mint push:send");
  }

  @Test
  void everySpecScopeRoundTrips() {
    String all = "tokens:mint sessions:revoke push:send push:broadcast presence:read devices:write admin";
    assertThat(Scope.format(Scope.parse(all))).isEqualTo(all);
    assertThat(Scope.parse(all)).isEqualTo(EnumSet.allOf(Scope.class));
  }
}
