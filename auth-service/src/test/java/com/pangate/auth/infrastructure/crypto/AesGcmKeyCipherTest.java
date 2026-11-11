package com.pangate.auth.infrastructure.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.starter.secrets.FileSecretSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AesGcmKeyCipherTest {

  static Kek kek(String id) {
    byte[] b = new byte[32];
    new SecureRandom().nextBytes(b);
    return new Kek(id, b);
  }

  private final Kek v1 = kek("v1");
  private final AesGcmKeyCipher cipher = new AesGcmKeyCipher(Map.of("v1", v1), "v1");
  private final byte[] plain = "private key bytes".getBytes(StandardCharsets.UTF_8);

  @Test
  void roundTrip() {
    byte[] sealed = cipher.encrypt("kid-1", plain);
    assertThat(sealed).hasSize(12 + plain.length + 16);
    assertThat(cipher.decrypt("v1", "kid-1", sealed)).isEqualTo(plain);
    assertThat(cipher.currentKekId()).isEqualTo("v1");
  }

  @Test
  void wrongKekFails() {
    byte[] sealed = cipher.encrypt("kid-1", plain);
    var other = new AesGcmKeyCipher(Map.of("v1", kek("v1")), "v1");
    assertThatThrownBy(() -> other.decrypt("v1", "kid-1", sealed)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> cipher.decrypt("v2", "kid-1", sealed)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("v2");
  }

  @Test
  void kidIsBoundAsAad() {
    byte[] sealed = cipher.encrypt("kid-1", plain);
    assertThatThrownBy(() -> cipher.decrypt("v1", "kid-2", sealed)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void kekMustBe32Bytes(@TempDir Path dir) throws Exception {
    Files.write(dir.resolve("auth.kek.v1"), new byte[31]);
    assertThatThrownBy(() -> Kek.load(new FileSecretSource(dir), "v1")).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("auth.kek.v1");
    Files.write(dir.resolve("auth.kek.v1"), new byte[32]);
    assertThat(Kek.load(new FileSecretSource(dir), "v1").id()).isEqualTo("v1");
  }
}
