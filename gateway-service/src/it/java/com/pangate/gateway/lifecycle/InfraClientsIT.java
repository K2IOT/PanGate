package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.config.RedisTopology;
import com.pangate.gateway.diagnostics.MissingTopicsException;
import com.pangate.gateway.it.InfraContainers;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class InfraClientsIT {

  static GatewayProperties.Redis redisCfg() {
    return new GatewayProperties.Redis(RedisTopology.STANDALONE, InfraContainers.redisUri(),
        InfraContainers.redisUri(), Duration.ofSeconds(2));
  }

  static GatewayProperties.Kafka kafkaCfg(String prefix) {
    return new GatewayProperties.Kafka(InfraContainers.kafkaBootstrap(), true, List.of("telemetry", "cmd"), prefix,
        DataSize.ofMegabytes(32), DataSize.ofMegabytes(32));
  }

  @Test
  void standaloneSharesOneConnectionWhenUrisEqual() throws Exception {
    try (RedisConnections redis = new RedisConnections(redisCfg())) {
      redis.connect();

      assertThat(redis.registry()).isSameAs(redis.state());
      assertThat(redis.registry().ping().get()).isEqualTo("PONG");
    }
  }

  @Test
  void commandsBeforeConnectFail() {
    RedisConnections redis = new RedisConnections(redisCfg());

    assertThatThrownBy(redis::registry).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void provisionerCreatesMissingTopicsIdempotently() throws Exception {
    TopicNames names = new TopicNames("p1", List.of("telemetry", "cmd"));
    try (KafkaClients kafka = new KafkaClients(kafkaCfg("p1"), "it")) {
      TopicProvisioner.ensure(kafka.admin(), names, true, Duration.ofSeconds(20));
      TopicProvisioner.ensure(kafka.admin(), names, true, Duration.ofSeconds(20));

      Set<String> topics = kafka.admin().listTopics().names().get();
      assertThat(topics).contains("p1.gw.lifecycle", "p1.gw.upstream.cmd", "p1.gw.upstream.telemetry", "gw.control");
    }
  }

  @Test
  void provisionerFailsListingMissingWhenCreateDisabled() {
    TopicNames names = new TopicNames("zz", List.of("cmd"));
    try (KafkaClients kafka = new KafkaClients(kafkaCfg("zz"), "it")) {
      assertThatThrownBy(() -> TopicProvisioner.ensure(kafka.admin(), names, false, Duration.ofSeconds(20)))
          .isInstanceOf(MissingTopicsException.class)
          .hasMessageContaining("zz.gw.lifecycle")
          .hasMessageContaining("zz.gw.upstream.cmd");
    }
  }

  @Test
  void producersReportBufferRatioNearZeroWhenIdle() {
    try (KafkaClients kafka = new KafkaClients(kafkaCfg("p2"), "it")) {
      kafka.startProducers();

      assertThat(kafka.maxBufferRatio()).isLessThan(0.01);
      assertThat(kafka.reliable()).isNotSameAs(kafka.telemetry());
    }
  }

  @Test
  void lettuceRunsOnNetty42() throws Exception {
    try (RedisConnections redis = new RedisConnections(redisCfg())) {
      redis.connect();
      assertThat(redis.registry().ping().get()).isEqualTo("PONG");
    }
    assertThat(io.netty.util.Version.identify().values().stream()
        .filter(v -> !v.artifactId().startsWith("netty-tcnative")).toList())
        .isNotEmpty().allSatisfy(v -> assertThat(v.artifactVersion()).startsWith("4.2.20"));
  }
}
