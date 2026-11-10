package com.pangate.auth.support;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** One Postgres and one Kafka per IT JVM, started lazily and reused by every Spring context. */
public final class Containers {

  private static PostgreSQLContainer<?> postgres;
  private static KafkaContainer kafka;

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
}
