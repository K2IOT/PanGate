package com.pangate.account.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.Test;

class LoginKeysTest {

  static final Set<String> RESERVED = Set.of("admin", "support");

  static String code(Runnable r) {
    try {
      r.run();
      return "ok";
    } catch (DomainException e) {
      return e.code();
    }
  }

  /** Review Focus 1: case and surrounding spaces never make a second key. */
  @Test
  void normalisationMakesOneKey() {
    assertThat(Email.of(" Lan@Mail.com ").value()).isEqualTo("lan@mail.com");
    Username u = Username.of("LanNguyen", RESERVED);
    assertThat(u.key()).isEqualTo("lannguyen");
    assertThat(u.display()).isEqualTo("LanNguyen");
    assertThat(Login.parse("LAN@mail.com")).isEqualTo(new Login(LoginKind.EMAIL, "lan@mail.com"));
    assertThat(Login.parse(" +84901234567 ")).isEqualTo(new Login(LoginKind.PHONE, "+84901234567"));
    assertThat(Login.parse("LanNguyen")).isEqualTo(new Login(LoginKind.USERNAME, "lannguyen"));
  }

  @Test
  void usernameRules() {
    assertThat(code(() -> Username.of("abcd", RESERVED))).isEqualTo("invalid_username");
    assertThat(code(() -> Username.of("abcde", RESERVED))).isEqualTo("ok");
    assertThat(code(() -> Username.of("a" + "b".repeat(31), RESERVED))).isEqualTo("ok");
    assertThat(code(() -> Username.of("a" + "b".repeat(32), RESERVED))).isEqualTo("invalid_username");
    assertThat(code(() -> Username.of("1abcde", RESERVED))).isEqualTo("invalid_username");
    assertThat(code(() -> Username.of("Admin", RESERVED))).isEqualTo("invalid_username");
    assertThat(code(() -> Username.of("a_b_c", RESERVED))).isEqualTo("ok");
    assertThat(code(() -> Username.of("ab-cde", RESERVED))).isEqualTo("invalid_username");
    assertThatThrownBy(() -> Username.of(null, RESERVED)).isInstanceOf(DomainException.class);
  }

  @Test
  void phoneAndEmailRules() {
    assertThat(code(() -> Phone.of("0901234567"))).isEqualTo("invalid_phone");
    assertThat(code(() -> Phone.of("+84901234567"))).isEqualTo("ok");
    assertThat(code(() -> Phone.of("+0123456789"))).isEqualTo("invalid_phone");
    assertThat(code(() -> Email.of("a".repeat(250) + "@b.vn"))).isEqualTo("invalid_email");   // 255
    assertThat(code(() -> Email.of("a".repeat(249) + "@b.vn"))).isEqualTo("ok");              // 254
    assertThat(code(() -> Email.of("a@b"))).isEqualTo("invalid_email");
    assertThat(code(() -> Email.of("a@@b.vn"))).isEqualTo("invalid_email");
    assertThat(code(() -> Email.of("a b@c.vn"))).isEqualTo("invalid_email");
    assertThat(code(() -> Email.of("a@b.vn"))).isEqualTo("ok");
  }
}
