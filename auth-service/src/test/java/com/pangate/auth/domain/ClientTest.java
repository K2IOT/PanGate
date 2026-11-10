package com.pangate.auth.domain;

import static com.pangate.auth.domain.ClaimLimitsTest.assertCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Set;
import org.junit.jupiter.api.Test;

class ClientTest {

  @Test
  void adminScopeOnlyForOperatorTenant() {
    assertCode(() -> Client.checkScopes("t1", Set.of(Scope.ADMIN)), "scope_not_allowed");
    assertThatCode(() -> Client.checkScopes("pangate", Set.of(Scope.ADMIN))).doesNotThrowAnyException();
    assertThatCode(() -> Client.checkScopes("t1", Set.of(Scope.TOKENS_MINT))).doesNotThrowAnyException();
  }

  @Test
  void operatorTenant() {
    assertThat(Tenant.isOperator("pangate")).isTrue();
    assertThat(Tenant.isOperator("t1")).isFalse();
    assertThat(Tenant.WARMUP).isEqualTo("__warmup");
  }
}
