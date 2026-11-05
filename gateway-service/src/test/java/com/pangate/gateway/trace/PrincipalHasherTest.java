package com.pangate.gateway.trace;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.starter.secrets.FileSecretSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PrincipalHasherTest {

  @Test
  void hashIsStable16HexAndKeyDependent(@TempDir Path d, @TempDir Path e) throws Exception {
    Files.writeString(d.resolve("principal-hash.key"), Base64.getEncoder().encodeToString(new byte[32]) + "\n");
    byte[] other = new byte[32];
    other[0] = 1;
    Files.writeString(e.resolve("principal-hash.key"), Base64.getEncoder().encodeToString(other));
    PrincipalHasher a = PrincipalHasher.load(new FileSecretSource(d));
    PrincipalHasher b = PrincipalHasher.load(new FileSecretSource(e));

    assertThat(a.hash("t1:u1")).matches("^[0-9a-f]{16}$").isEqualTo(a.hash("t1:u1"));
    assertThat(a.hash("t1:u1")).isNotEqualTo(a.hash("t1:u2"));
    assertThat(a.hash("t1:u1")).isNotEqualTo(b.hash("t1:u1"));
  }
}
