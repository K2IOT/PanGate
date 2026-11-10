package com.pangate.auth.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** One Postgres and one Kafka per IT JVM, started lazily and reused by every Spring context. */
public final class Containers {

  private static PostgreSQLContainer<?> postgres;
  private static KafkaContainer kafka;

  private static Path secrets;

  private Containers() {}

  /** A secret directory with KEK {@code auth.kek.v1}, shared by every context so all of them can open keys. */
  public static synchronized Path secretsDir() {
    if (secrets == null) {
      try {
        secrets = Files.createTempDirectory("auth-it-secrets");
        byte[] kek = new byte[32];
        new java.security.SecureRandom().nextBytes(kek);
        Files.write(secrets.resolve("auth.kek.v1"), kek);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return secrets;
  }

  private static final java.util.Set<String> created = new java.util.HashSet<>();

  /** Like {@link #freshDatabase} the first time per JVM, then the same database (for restart tests). */
  public static synchronized String freshDatabaseOnce(String name) {
    if (created.add(name)) {
      return freshDatabase(name);
    }
    var pg = postgres();
    return "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/" + name;
  }

  /** JDBC URL of a fresh database {@code name} in the shared container (dropped and recreated). */
  public static String freshDatabase(String name) {
    var pg = postgres();
    try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        var st = c.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + name);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    return "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/" + name;
  }

  public static synchronized PostgreSQLContainer<?> postgres() {
    if (postgres == null) {
      postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4"))
          .withDatabaseName("pangate").withUsername("pangate").withPassword("pangate");
      postgres.start();
    }
    return postgres;
  }

  public static synchronized KafkaContainer kafka() {
    if (kafka == null) {
      kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
      kafka.start();
    }
    return kafka;
  }
}
