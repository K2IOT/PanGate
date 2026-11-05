package com.pangate.gateway.diagnostics;

import java.util.Set;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Spec §9.1, §10: checks the JVM flags the active profile needs before any bean is created, so a prod pod without
 * {@code -XX:MaxDirectMemorySize} reports that, not whichever bean happened to fail first.
 */
public final class JvmSettingsEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
    JvmSettings.read().check(Set.of(environment.getActiveProfiles()));
  }

  @Override
  public int getOrder() {
    return Ordered.HIGHEST_PRECEDENCE + 100;
  }
}
