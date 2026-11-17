package com.pangate.notify.domain;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Offline notifications spec §5.1: what a push token looks like. */
class TokenRulesTest {

  static void ok(Platform p, String token, String env) {
    assertThatCode(() -> TokenRules.validate(p, token, env)).doesNotThrowAnyException();
  }

  static void refused(Platform p, String token, String env) {
    assertThatThrownBy(() -> TokenRules.validate(p, token, env)).isInstanceOf(DomainException.class)
        .extracting(e -> ((DomainException) e).code()).isEqualTo("invalid_request");
  }

  @Test
  void apnsTokenIsHexOf64To200() {
    refused(Platform.APNS, "a".repeat(63), "production");
    ok(Platform.APNS, "a".repeat(64), "production");
    ok(Platform.APNS, "0123456789abcdefABCDEF".repeat(10).substring(0, 200), "sandbox");
    refused(Platform.APNS, "a".repeat(201), "production");
    refused(Platform.APNS, "g".repeat(64), "production");
  }

  @Test
  void fcmTokenUpTo4096() {
    ok(Platform.FCM, "x".repeat(4_096), "production");
    refused(Platform.FCM, "x".repeat(4_097), "production");
    refused(Platform.FCM, "", "production");
  }

  @Test
  void environmentPerPlatform() {
    refused(Platform.APNS, "a".repeat(64), "staging");
    refused(Platform.FCM, "x".repeat(100), "sandbox");
  }
}
