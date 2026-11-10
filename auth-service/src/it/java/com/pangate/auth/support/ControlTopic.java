package com.pangate.auth.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads gw.control from the beginning (records of every test class; callers filter by key). */
public final class ControlTopic {

  private ControlTopic() {}

  public static List<ConsumerRecord<String, byte[]>> read(String topic) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafka().getBootstrapServers());
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
    try (var c = new KafkaConsumer<String, byte[]>(p)) {
      var infos = c.partitionsFor(topic, Duration.ofSeconds(10));
      if (infos == null || infos.isEmpty()) {
        return out;
      }
      List<TopicPartition> parts = infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
      c.assign(parts);
      c.seekToBeginning(parts);
      Map<TopicPartition, Long> end = c.endOffsets(parts);
      while (parts.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
        c.poll(Duration.ofMillis(500)).forEach(out::add);
      }
    }
    return out;
  }
}
