package com.pangate.gateway.diagnostics;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.StandardEnvironment;

/** The prod check runs before any bean, so it is reported even when other start-up problems exist (spec §10). */
class JvmSettingsEnvironmentPostProcessorTest {

  private static StandardEnvironment env(String profile) {
    StandardEnvironment env = new StandardEnvironment();
    env.setActiveProfiles(profile);
    return env;
  }

  @Test
  void prodWithoutDirectMemoryFailsBeforeBeans() {
    // surefire forks without -XX:MaxDirectMemorySize
    assertThatThrownBy(() -> new JvmSettingsEnvironmentPostProcessor()
        .postProcessEnvironment(env("prod"), new SpringApplication())).isInstanceOf(JvmConfigException.class);
  }

  @Test
  void devStarts() {
    assertThatCode(() -> new JvmSettingsEnvironmentPostProcessor()
        .postProcessEnvironment(env("dev"), new SpringApplication())).doesNotThrowAnyException();
  }
}
