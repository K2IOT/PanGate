package com.pangate.account.support;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads a topic of the shared Kafka from the beginning until a record matches. */
public final class ControlTopic {

  private ControlTopic() {}

  public static List<ConsumerRecord<String, byte[]>> await(String topic, Predicate<List<ConsumerRecord<String, byte[]>>>
      done, Duration within) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafka().getBootstrapServers());
    cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<ConsumerRecord<String, byte[]>> seen = new java.util.ArrayList<>();
    try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(cfg, new StringDeserializer(),
        new ByteArrayDeserializer())) {
      c.subscribe(List.of(topic));
      long deadline = System.nanoTime() + within.toNanos();
      while (System.nanoTime() < deadline) {
        c.poll(Duration.ofMillis(200)).forEach(seen::add);
        if (done.test(seen)) {
          return seen;
        }
      }
    }
    return seen;
  }

  /** True once {@code key} appeared. */
  public static boolean hasKey(String topic, String key, Duration within) {
    return await(topic, rs -> rs.stream().anyMatch(r -> key.equals(r.key())), within).stream()
        .anyMatch(r -> key.equals(r.key()));
  }
}
