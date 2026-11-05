package com.pangate.gateway.it;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/** Redis and Kafka shared by every IT in the failsafe JVM (started once, lazily; Ryuk removes them at exit). */
public final class InfraContainers {

  private static GenericContainer<?> redis;
  private static KafkaContainer kafka;

  private InfraContainers() {}

  private static synchronized void start() {
    if (redis == null) {
      redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2")).withExposedPorts(6379);
      kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
      Startables.deepStart(redis, kafka).join();
    }
  }

  public static GenericContainer<?> redis() {
    start();
    return redis;
  }

  public static KafkaContainer kafka() {
    start();
    return kafka;
  }

  public static String redisUri() {
    return "redis://" + redis().getHost() + ":" + redis().getMappedPort(6379);
  }

  public static String kafkaBootstrap() {
    return kafka().getBootstrapServers();
  }

  /** Command-line overrides pointing a gateway started with profile local at the containers. */
  public static List<String> gatewayArgs() {
    return List.of("--gateway.redis.registry-uri=" + redisUri(), "--gateway.redis.state-uri=" + redisUri(),
        "--gateway.kafka.bootstrap=" + kafkaBootstrap());
  }

  public static void flushRedis() {
    RedisClient client = RedisClient.create(redisUri());
    try (StatefulRedisConnection<String, String> c = client.connect()) {
      c.sync().flushall();
    } finally {
      client.shutdown();
    }
  }
}
