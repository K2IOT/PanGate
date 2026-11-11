package com.pangate.devtools.push;

import com.pangate.contracts.testkit.PushTopics;
import com.pangate.contracts.v1.OfflineRecord;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads {@code cN.push.offline} without a consumer group, waiting for one msg_id. */
public final class OfflineAwaiter implements AutoCloseable {

  private final KafkaConsumer<String, byte[]> consumer;

  /** {@code fromBeginning=false}: only records written after this constructor returns. */
  public OfflineAwaiter(String bootstrap, String cell, boolean fromBeginning) {
    consumer = new KafkaConsumer<>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ConsumerConfig.CLIENT_ID_CONFIG, "devtools-offline-" + UUID.randomUUID(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new ByteArrayDeserializer());
    String topic = PushTopics.offline(cell).name();
    List<TopicPartition> parts = consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
        .map(p -> new TopicPartition(topic, p.partition())).toList();
    consumer.assign(parts);
    if (fromBeginning) {
      consumer.seekToBeginning(parts);
    } else {
      consumer.seekToEnd(parts);
      parts.forEach(consumer::position);   // resolve now, before the caller produces
    }
  }

  public OfflineRecord await(String msgId, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      for (ConsumerRecord<String, byte[]> rec : consumer.poll(Duration.ofMillis(200))) {
        try {
          OfflineRecord o = OfflineRecord.parseFrom(rec.value());
          if (o.getRecord().getMsgId().equals(msgId)) {
            return o;
          }
        } catch (Exception ignored) {
          // not an OfflineRecord: skip
        }
      }
    }
    throw new IllegalStateException("no OfflineRecord for " + msgId + " within " + timeout);
  }

  @Override
  public void close() {
    consumer.close(Duration.ofSeconds(2));
  }
}
