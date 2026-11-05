package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.Revoke;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.it.InfraContainers;
import com.pangate.gateway.lifecycle.KafkaClients;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.test.TestLimits;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class ControlEventConsumerIT {

  private final String topic = "ctl-" + UUID.randomUUID();
  private final RevocationList revocations = new RevocationList();
  private final NodeState node = new NodeState();
  private ControlEventConsumer consumer;

  private KafkaClients clients() {
    return new KafkaClients(new GatewayProperties.Kafka(InfraContainers.kafkaBootstrap(), true, List.of(), "c1",
        DataSize.ofMegabytes(8), DataSize.ofMegabytes(8)), "it-" + UUID.randomUUID());
  }

  private void createTopic() throws Exception {
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", InfraContainers.kafkaBootstrap()))) {
      admin.createTopics(List.of(new NewTopic(topic, Optional.of(3), Optional.empty())
          .configs(Map.of("cleanup.policy", "compact")))).all().get();
    }
  }

  private void publish(String key, byte[] value) throws Exception {
    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
        InfraContainers.kafkaBootstrap()), new StringSerializer(), new ByteArraySerializer())) {
      p.send(new ProducerRecord<>(topic, key, value)).get();
    }
  }

  private static byte[] revoke(String principal) {
    return ControlEvent.newBuilder().setRevoke(Revoke.newBuilder().setPrincipalId(principal)
        .setExpiresAtMs(Long.MAX_VALUE)).build().toByteArray();
  }

  private void start() {
    ControlState state = new ControlState(revocations, new CellMapView("cell-1"),
        new TenantLimitsStore("cell-1", TestLimits::defaults), System::currentTimeMillis);
    KafkaClients k = clients();
    consumer = new ControlEventConsumer(k::newControlConsumer, topic, node, List.of(state),
        new GatewayMetrics(new SimpleMeterRegistry()), (s, r) -> { }, Duration.ofMinutes(5),
        () -> System.nanoTime() / 1_000_000);
    consumer.start();
  }

  @AfterEach
  void stop() {
    if (consumer != null) {
      consumer.stop();
    }
  }

  @Test
  void caughtUpImmediatelyOnEmptyTopic() throws Exception {
    createTopic();
    start();

    await().atMost(Duration.ofSeconds(15)).until(node::controlCaughtUp);
  }

  @Test
  void replaysCompactedStateBeforeCaughtUp() throws Exception {
    createTopic();
    publish("revoke:p:t1:u9", revoke("t1:u9"));
    publish("revoke:p:t1:u8", revoke("t1:u8"));

    start();

    await().atMost(Duration.ofSeconds(15)).until(node::controlCaughtUp);
    assertThat(revocations.isRevoked("t1:u9", "d", 0, System.currentTimeMillis())).isTrue();
    assertThat(revocations.isRevoked("t1:u8", "d", 0, System.currentTimeMillis())).isTrue();
  }

  @Test
  void tombstoneRemovesStateAndLiveEventsApply() throws Exception {
    createTopic();
    publish("revoke:p:t1:u7", revoke("t1:u7"));
    start();
    await().atMost(Duration.ofSeconds(15)).until(node::controlCaughtUp);

    publish("revoke:p:t1:u7", null);
    publish("revoke:p:t1:u6", revoke("t1:u6"));

    await().atMost(Duration.ofSeconds(10)).until(() ->
        !revocations.isRevoked("t1:u7", "d", 0, System.currentTimeMillis())
            && revocations.isRevoked("t1:u6", "d", 0, System.currentTimeMillis()));
  }
}
