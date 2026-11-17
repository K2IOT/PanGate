package com.pangate.account.support;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** One Postgres, Kafka and Redis per IT JVM, started lazily and reused by every Spring context. */
public final class Containers {

  private static PostgreSQLContainer<?> postgres;
  private static KafkaContainer kafka;
  private static GenericContainer<?> redis;

  private Containers() {}

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

  public static synchronized GenericContainer<?> redis() {
    if (redis == null) {
      redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2")).withExposedPorts(6379);
      redis.start();
    }
    return redis;
  }

  private static final java.util.Set<String> databases = new java.util.HashSet<>();

  /** JDBC URL of database {@code name} in the shared container, created (empty) the first time it is asked for. */
  public static synchronized String database(String name) {
    var pg = postgres();
    if (databases.add(name)) {
      try (var c = java.sql.DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
          var st = c.createStatement()) {
        st.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
        st.execute("CREATE DATABASE " + name);
      } catch (java.sql.SQLException e) {
        throw new IllegalStateException(e);
      }
    }
    return "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/" + name;
  }

  public static String redisUri() {
    return "redis://" + redis().getHost() + ":" + redis().getMappedPort(6379);
  }
}
