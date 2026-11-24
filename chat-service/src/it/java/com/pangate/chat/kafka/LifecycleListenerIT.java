package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.PresenceTracker;
import com.pangate.chat.store.RedisPresence;
import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** CH3 spec §6.1: c1.gw.lifecycle → presence. */
class LifecycleListenerIT {

  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);

  @BeforeAll
  static void boot() throws Exception {
    kafka.start();
    redis.start();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic("c1.gw.lifecycle", 2, (short) 1))).all().get();
    }
  }

  @AfterAll
  static void shutdown() {
    kafka.stop();
    redis.stop();
  }

  @Test
  void connectedEventMakesUserOnline() {
    RedisPresence presence = new RedisPresence(new ChatProperties.Redis("redis://" + redis.getHost() + ":"
        + redis.getMappedPort(6379), Duration.ofSeconds(1), Duration.ofSeconds(60)));
    List<PresenceTracker.Transition> out = new CopyOnWriteArrayList<>();
    PresenceTracker tracker = new PresenceTracker(presence, new FakeStores(), out::add, System::currentTimeMillis,
        Duration.ofSeconds(5));
    try (presence; LifecycleListener l = new LifecycleListener(LifecycleListener.consumer(kafka.getBootstrapServers(),
        "lc-" + Ids.next()), "c1.gw.lifecycle", tracker, new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers", kafka.getBootstrapServers()),
            new StringSerializer(), new ByteArraySerializer())) {
      l.start();
      p.send(new ProducerRecord<>("c1.gw.lifecycle", "chat:lc1", ConnectionEvent.newBuilder()
          .setType(EventType.EVENT_TYPE_CONNECTED).setPrincipalId("chat:lc1").setTenantId("chat").setNodeId("n1")
          .setEpoch(1).setConnId("c1").setEventId(Ids.next()).setAtMs(System.currentTimeMillis()).build()
          .toByteArray()));
      p.send(new ProducerRecord<>("c1.gw.lifecycle", "chat:lc1", new byte[] {(byte) 0xff, 1}));   // malformed: skipped
      p.flush();
      // the tracker writes Redis first and reports the transition after: wait for both, not just the first
      await().atMost(Duration.ofSeconds(20)).until(() -> presence.online("lc1") && !out.isEmpty());
      assertThat(out).extracting(PresenceTracker.Transition::userId).containsExactly("lc1");
    }
  }
}
