package com.pangate.starter.secrets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSecretSourceTest {

  @Test
  void readsNestedSecret(@TempDir Path dir) throws Exception {
    Files.createDirectories(dir.resolve("ticket-keys"));
    Files.writeString(dir.resolve("ticket-keys/current"), "2 abc");

    byte[] value = new FileSecretSource(dir).get("ticket-keys/current");

    assertThat(new String(value, StandardCharsets.UTF_8)).isEqualTo("2 abc");
  }

  @Test
  void missingSecretThrowsWithPath(@TempDir Path dir) {
    assertThatThrownBy(() -> new FileSecretSource(dir).get("gateway.key"))
        .isInstanceOf(SecretNotFoundException.class)
        .hasMessageContaining("gateway.key")
        .hasMessageContaining(dir.toString());
  }

  @Test
  void missingSecretExposesNameAndLocation(@TempDir Path dir) {
    assertThatThrownBy(() -> new FileSecretSource(dir).get("gateway.key"))
        .isInstanceOfSatisfying(SecretNotFoundException.class, e -> {
          assertThat(e.secretName()).isEqualTo("gateway.key");
          assertThat(e.location()).isEqualTo(dir.resolve("gateway.key").toString());
        });
  }

  @Test
  void rejectsPathTraversal(@TempDir Path dir) {
    assertThatThrownBy(() -> new FileSecretSource(dir).get("../x"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void describeReturnsFilePath(@TempDir Path dir) {
    assertThat(new FileSecretSource(dir).describe("a/b")).isEqualTo(dir.resolve("a/b").toString());
  }
}
