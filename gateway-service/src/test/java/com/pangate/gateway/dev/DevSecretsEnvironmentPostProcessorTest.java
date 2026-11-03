package com.pangate.gateway.dev;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

class DevSecretsEnvironmentPostProcessorTest {

  private final DevSecretsEnvironmentPostProcessor processor = new DevSecretsEnvironmentPostProcessor();

  private static StandardEnvironment env(Path dir, boolean bootstrap, String... profiles) {
    StandardEnvironment env = new StandardEnvironment();
    env.getPropertySources().addFirst(new MapPropertySource("test", Map.of(
        "gateway.dev.bootstrap-secrets", String.valueOf(bootstrap), "gateway.dev.dir", dir.toString())));
    env.setActiveProfiles(profiles);
    return env;
  }

  @Test
  void localProfileGeneratesSecrets(@TempDir Path d) {
    processor.postProcessEnvironment(env(d, true, "local"), new SpringApplication());

    assertThat(d.resolve("gateway.crt")).exists();
  }

  @Test
  void prodProfileWithBootstrapFails(@TempDir Path d) {
    assertThatThrownBy(() -> processor.postProcessEnvironment(env(d, true, "prod"), new SpringApplication()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("gateway.dev.bootstrap-secrets must not be enabled with profile prod");
  }

  @Test
  void noProfileDoesNothing(@TempDir Path d) {
    processor.postProcessEnvironment(env(d, true), new SpringApplication());

    assertThat(d.resolve("gateway.crt")).doesNotExist();
  }

  @Test
  void flagOffDoesNothing(@TempDir Path d) {
    processor.postProcessEnvironment(env(d, false, "local"), new SpringApplication());

    assertThat(d.resolve("gateway.crt")).doesNotExist();
  }
}
