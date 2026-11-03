package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.config.RedisTopology;
import com.pangate.gateway.diagnostics.DependencyFailureAnalyzer;
import com.pangate.gateway.diagnostics.DependencyUnavailableException;
import com.pangate.gateway.it.InfraContainers;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class InfraLifecycleIT {

  private static InfraLifecycle lifecycle(String redisUri, String bootstrap, String prefix, Duration timeout) {
    GatewayProperties.Redis r = new GatewayProperties.Redis(RedisTopology.STANDALONE, redisUri, redisUri,
        Duration.ofSeconds(2));
    GatewayProperties.Kafka k = new GatewayProperties.Kafka(bootstrap, true, List.of("telemetry", "cmd"), prefix,
        DataSize.ofMegabytes(16), DataSize.ofMegabytes(16));
    return new InfraLifecycle(timeout, new RedisConnections(r), new KafkaClients(k, "it"),
        new TopicNames(prefix, k.upstreamTypes()), true);
  }

  @Test
  void startsAgainstContainersAndCreatesTopics() throws Exception {
    InfraLifecycle l = lifecycle(InfraContainers.redisUri(), InfraContainers.kafkaBootstrap(), "s1",
        Duration.ofSeconds(30));
    try {
      l.start();

      assertThat(l.isRunning()).isTrue();
      assertThat(l.kafka().admin().listTopics().names().get()).contains("s1.gw.lifecycle", "s1.gw.upstream.cmd");
      assertThat(l.redis().registry().ping().get()).isEqualTo("PONG");
      assertThat(l.getPhase()).isEqualTo(Integer.MAX_VALUE - 1900);
    } finally {
      l.stop();
    }
    assertThat(l.isRunning()).isFalse();
  }

  @Test
  void kafkaUnreachableFailsWithinTimeoutWithDiagnostic() {
    InfraLifecycle l = lifecycle(InfraContainers.redisUri(), "localhost:1", "s2", Duration.ofSeconds(3));
    long t0 = System.nanoTime();

    assertThatThrownBy(l::start).isInstanceOfSatisfying(DependencyUnavailableException.class, e ->
        assertThat(new DependencyFailureAnalyzer().analyze(e).getDescription()).isEqualTo("Kafka localhost:1 is not reachable after 3 s"));
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(8));
    l.stop();
  }

  @Test
  void redisUnreachableFailsWithinTimeoutWithDiagnostic() {
    InfraLifecycle l = lifecycle("redis://localhost:1", InfraContainers.kafkaBootstrap(), "s3",
        Duration.ofSeconds(3));
    long t0 = System.nanoTime();

    assertThatThrownBy(l::start).isInstanceOfSatisfying(DependencyUnavailableException.class, e ->
        assertThat(new DependencyFailureAnalyzer().analyze(e).getDescription()).isEqualTo("Redis redis://localhost:1 is not reachable after 3 s"));
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(8));
    l.stop();
  }
}
