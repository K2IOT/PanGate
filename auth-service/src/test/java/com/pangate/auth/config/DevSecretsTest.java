package com.pangate.auth.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

class DevSecretsTest {

  @Test
  void localProfileCreatesTheKekOnceAndPointsTheSecretSourceAtIt(@TempDir Path dir) throws Exception {
    var env = new MockEnvironment().withProperty("auth.seed.dir", dir.resolve("auth").toString());
    env.setActiveProfiles("local");
    new DevSecrets().postProcessEnvironment(env, null);

    Path kek = dir.resolve("auth/auth.kek.v1");
    assertThat(Files.readAllBytes(kek)).hasSize(32);
    assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(kek))).isEqualTo("rw-------");
    assertThat(env.getProperty("pangate.secrets.dir")).isEqualTo(dir.resolve("auth").toString());

    byte[] first = Files.readAllBytes(kek);
    new DevSecrets().postProcessEnvironment(env, null);
    assertThat(Files.readAllBytes(kek)).isEqualTo(first);
  }

  @Test
  void otherProfilesAreUntouched(@TempDir Path dir) {
    var env = new MockEnvironment().withProperty("auth.seed.dir", dir.toString());
    env.setActiveProfiles("prod");
    new DevSecrets().postProcessEnvironment(env, null);
    assertThat(dir.resolve("auth.kek.v1")).doesNotExist();
    assertThat(env.getProperty("pangate.secrets.dir")).isNull();
  }

  @Test
  void anExplicitSecretsDirWins(@TempDir Path dir) {
    var env = new MockEnvironment().withProperty("auth.seed.dir", dir.toString())
        .withProperty("pangate.secrets.dir", "/elsewhere");
    env.setActiveProfiles("dev");
    new DevSecrets().postProcessEnvironment(env, null);
    assertThat(env.getProperty("pangate.secrets.dir")).isEqualTo("/elsewhere");
    assertThat(dir.resolve("auth.kek.v1")).doesNotExist();
  }
}
