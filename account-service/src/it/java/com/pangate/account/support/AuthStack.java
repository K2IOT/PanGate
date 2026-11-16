package com.pangate.account.support;

import com.pangate.auth.AuthApplication;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The real auth-service in the test JVM, once per JVM, against the shared Postgres (schema auth) and Kafka; seed on,
 * so {@code clients.json} holds chat-backend and chat-admin. Its application.yml is shadowed by account-service's on
 * the test class path, so it starts with {@code spring.config.name} pointing nowhere and every setting as an argument
 * (AuthProperties has defaults for the rest).
 */
public final class AuthStack {

  private static ConfigurableApplicationContext app;
  private static String baseUrl;
  private static Path seedDir;

  private AuthStack() {}

  static int freePort() {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static synchronized void start() {
    if (app != null) {
      return;
    }
    try {
      Path secrets = Files.createTempDirectory("account-it-auth-secrets");
      byte[] kek = new byte[32];
      new SecureRandom().nextBytes(kek);
      Files.write(secrets.resolve("auth.kek.v1"), kek);
      seedDir = Files.createTempDirectory("account-it-auth-seed");
      int port = freePort();
      baseUrl = "http://127.0.0.1:" + port;
      var pg = Containers.postgres();
      app = new SpringApplicationBuilder(AuthApplication.class).run(
          "--spring.config.name=auth-it-no-config-file",
          "--spring.application.name=auth-service",
          "--spring.threads.virtual.enabled=true",
          "--spring.datasource.url=" + pg.getJdbcUrl(),
          "--spring.datasource.username=" + pg.getUsername(),
          "--spring.datasource.password=" + pg.getPassword(),
          "--spring.flyway.schemas=auth",
          "--spring.flyway.default-schema=auth",
          "--server.port=" + port,
          "--auth.admin-port=" + freePort(),
          "--auth.issuer=" + baseUrl,
          "--auth.kafka.bootstrap-servers=" + Containers.kafka().getBootstrapServers(),
          "--auth.seed.enabled=true",
          "--auth.seed.dir=" + seedDir,
          "--pangate.secrets.dir=" + secrets,
          "--pangate.service=auth",
          "--management.endpoints.web.exposure.include=health");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String baseUrl() {
    start();
    return baseUrl;
  }

  public static Path clientsFile() {
    start();
    return seedDir.resolve("clients.json");
  }
}
