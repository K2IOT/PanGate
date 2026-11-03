package com.pangate.gateway.dev;

import java.nio.file.Path;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;

/**
 * Generates missing dev secrets before any bean reads them (profiles local and dev only). Refuses to run with
 * profile prod so a dev CA can never reach production.
 */
public final class DevSecretsEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
    Binder binder = Binder.get(environment);
    boolean enabled = binder.bind("gateway.dev.bootstrap-secrets", Boolean.class).orElse(false);
    if (!enabled) {
      return;
    }
    if (environment.acceptsProfiles(Profiles.of("prod"))) {
      throw new IllegalStateException("gateway.dev.bootstrap-secrets must not be enabled with profile prod");
    }
    if (environment.acceptsProfiles(Profiles.of("local", "dev"))) {
      Path dir = binder.bind("gateway.dev.dir", Path.class)
          .orElseThrow(() -> new IllegalStateException("gateway.dev.dir is required when bootstrap-secrets is on"));
      DevSecretsGenerator.ensure(dir);
    }
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }
}
