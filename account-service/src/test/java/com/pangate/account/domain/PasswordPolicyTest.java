package com.pangate.account.domain;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.infrastructure.crypto.Argon2PasswordHasher;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PasswordPolicyTest {

  final Username user = Username.of("LanNguyen", Set.of());

  String code(String pw, Email e, Phone p) {
    try {
      PasswordPolicy.check(pw.toCharArray(), user, e, p);
      return "ok";
    } catch (DomainException ex) {
      return ex.code();
    }
  }

  @Test
  void lengthIsCountedInCharacters() {
    assertThat(code("a".repeat(7), null, null)).isEqualTo("weak_password");
    assertThat(code("a".repeat(8), null, null)).isEqualTo("ok");
    assertThat(code("a".repeat(128), null, null)).isEqualTo("ok");
    assertThat(code("a".repeat(129), null, null)).isEqualTo("weak_password");
    assertThat(code("🔐".repeat(8), null, null)).as("8 code points, 16 UTF-16 units").isEqualTo("ok");
  }

  /** Review Focus 2: é typed as one code point or as e + combining accent is the same password. */
  @Test
  void nfcAndNfdAreTheSamePassword() {
    Argon2PasswordHasher h = new Argon2PasswordHasher();
    String hash = h.hash(PasswordPolicy.check("café123x".toCharArray(), user, null, null));
    assertThat(h.matches(PasswordPolicy.normalise("café123x".toCharArray()), hash)).isTrue();
    assertThat(h.matches(PasswordPolicy.normalise("cafe123x".toCharArray()), hash)).isFalse();
  }

  @Test
  void mustNotEqualIdentifiers() {
    assertThat(code("lannguyen", null, null)).isEqualTo("weak_password");
    assertThat(code("LANNGUYEN", null, null)).isEqualTo("weak_password");
    assertThat(code("lan@mail.com", Email.of("lan@mail.com"), null)).isEqualTo("weak_password");
    assertThat(code("+84901234567", null, Phone.of("+84901234567"))).isEqualTo("weak_password");
    assertThat(code("lannguyen1", null, null)).isEqualTo("ok");
  }
}
