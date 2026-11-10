package com.pangate.auth.domain;

import static com.pangate.auth.domain.ClaimLimits.validate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClaimLimitsTest {

  static DomainException assertCode(Runnable r, String code) {
    DomainException e = catchThrowableOfType(DomainException.class, r::run);
    assertThat(e).as("expected " + code).isNotNull();
    assertThat(e.code()).isEqualTo(code);
    return e;
  }

  @Test
  void principalMustBeInsideTenant() {
    assertCode(() -> validate("t1", "t2:u", "d", List.of(), null), "principal_outside_tenant");
    assertCode(() -> validate("t1", "t1", "d", List.of(), null), "principal_outside_tenant");
    assertCode(() -> validate("t1", null, "d", List.of(), null), "principal_outside_tenant");
    assertCode(() -> validate("t1", "t1:", "d", List.of(), null), "principal_outside_tenant");
  }

  @Test
  void principalLimitIsUtf8Bytes() {
    String ok = "t1:" + "a".repeat(253);
    String tooLong = "t1:" + "a".repeat(252) + "é";
    assertThat(validate("t1", ok, "d", List.of(), null).principal()).isEqualTo(ok);
    assertThat(tooLong.length()).isEqualTo(256);
    assertCode(() -> validate("t1", tooLong, "d", List.of(), null), "principal_too_long");
  }

  @Test
  void deviceRequiredAndAtMost128Bytes() {
    assertCode(() -> validate("t1", "t1:u", "", List.of(), null), "device_invalid");
    assertCode(() -> validate("t1", "t1:u", null, List.of(), null), "device_invalid");
    assertCode(() -> validate("t1", "t1:u", "d".repeat(127) + "é", List.of(), null), "device_invalid");
    assertThat(validate("t1", "t1:u", "d".repeat(128), List.of(), null).device()).hasSize(128);
  }

  @Test
  void topics() {
    assertCode(() -> validate("t1", "t1:u", "d", Collections.nCopies(9, "t1:x"), null), "too_many_topics");
    assertThat(validate("t1", "t1:u", "d", Collections.nCopies(8, "t1:x"), null).topics()).hasSize(8);
    assertCode(() -> validate("t1", "t1:u", "d", List.of("t2:x"), null), "topic_outside_tenant");
    assertCode(() -> validate("t1", "t1:u", "d", List.of("t1:" + "a".repeat(62)), null), "topic_pattern_too_long");
    assertThat(validate("t1", "t1:u", "d", List.of("t1:" + "a".repeat(61)), null).topics()).hasSize(1);
    assertThat(validate("t1", "t1:u", "d", null, null).topics()).isEmpty();
  }

  @Test
  void ttl() {
    assertThat(validate("t1", "t1:u", "d", List.of(), null).ttlSec()).isEqualTo(600);
    assertCode(() -> validate("t1", "t1:u", "d", List.of(), 59), "ttl_out_of_range");
    assertCode(() -> validate("t1", "t1:u", "d", List.of(), 601), "ttl_out_of_range");
    assertThat(validate("t1", "t1:u", "d", List.of(), 60).ttlSec()).isEqualTo(60);
    assertThat(validate("t1", "t1:u", "d", List.of(), 600).ttlSec()).isEqualTo(600);
  }

  @Test
  void warmupAndOperatorTenantsCannotMint() {
    assertThat(assertCode(() -> validate("__warmup", "__warmup:u", "d", List.of(), null), "tenant_not_allowed")
        .httpStatus()).isEqualTo(403);
    assertThat(assertCode(() -> validate("pangate", "pangate:u", "d", List.of(), null), "tenant_not_allowed")
        .httpStatus()).isEqualTo(403);
  }

  @Test
  void tenantClaimIsBoundedAndHasNoColon() {
    assertCode(() -> validate("a".repeat(65), "x:u", "d", List.of(), null), "tenant_invalid");
    assertCode(() -> validate("t:1", "t:1:u", "d", List.of(), null), "tenant_invalid");
    assertCode(() -> validate("", ":u", "d", List.of(), null), "tenant_invalid");
  }

  @Test
  void violationsAre400() {
    assertThat(assertCode(() -> validate("t1", "t2:u", "d", List.of(), null), "principal_outside_tenant")
        .httpStatus()).isEqualTo(400);
  }
}
