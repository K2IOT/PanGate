package com.pangate.auth.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Profiles {@code local} and {@code dev} only: creates KEK {@code auth.kek.v1} (32 random bytes, mode 600) in the
 * seed directory on first run and points the file secret source there, unless {@code pangate.secrets.dir} is set
 * explicitly (spec §8). Like the gateway's DevBootstrap, nothing here runs in prod.
 */
public class DevSecrets implements EnvironmentPostProcessor, Ordered {

  static final Set<String> PROFILES = Set.of("local", "dev");

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication app) {
    boolean dev = false;
    for (String p : env.getActiveProfiles()) {
      dev |= PROFILES.contains(p);
    }
    if (!dev || env.containsProperty("pangate.secrets.dir")) {
      return;
    }
    Path dir = Path.of(env.getProperty("auth.seed.dir", ".dev/auth"));
    String kekId = env.getProperty("auth.keys.kek-id", "v1");
    Path kek = dir.resolve("auth.kek." + kekId);
    try {
      Files.createDirectories(dir);
      if (!Files.exists(kek)) {
        byte[] b = new byte[32];
        new SecureRandom().nextBytes(b);
        Path tmp = Files.createTempFile(dir, "kek", ".tmp",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.write(tmp, b);
        Files.move(tmp, kek);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("cannot create dev KEK " + kek, e);
    }
    env.getPropertySources().addLast(new MapPropertySource("authDevSecrets",
        Map.of("pangate.secrets.dir", dir.toString())));
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }
}
