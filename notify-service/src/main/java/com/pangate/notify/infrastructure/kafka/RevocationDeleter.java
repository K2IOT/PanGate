package com.pangate.notify.infrastructure.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.notify.application.TokenRevoker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline notifications spec §5.3: consumer group {@code notify-revocations} on {@code gw.control} (one group for
 * every pod: a token is removed once). Commits after the batch is applied; the store failing rewinds to the last
 * commit after a growing pause (100 ms to 5 s), so no revocation is skipped and the thread never dies.
 */
public final class RevocationDeleter implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RevocationDeleter.class);
  public static final String GROUP = "notify-revocations";
  static final long MAX_BACKOFF_MS = 5_000;

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final TokenRevoker revoker;
  private final Runnable onMalformed;
  private volatile boolean running = true;
  private Thread thread;

  public RevocationDeleter(Consumer<String, byte[]> consumer, String topic, TokenRevoker revoker,
      Runnable onMalformed) {
    this.consumer = consumer;
    this.topic = topic;
    this.revoker = revoker;
    this.onMalformed = onMalformed;
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap) {
    return new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ConsumerConfig.GROUP_ID_CONFIG, GROUP,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500), new StringDeserializer(), new ByteArrayDeserializer());
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("notify-revocation-deleter").start(this::run);
  }

  private void run() {
    long backoff = 100;
    try {
      consumer.subscribe(List.of(topic));
      while (running) {
        ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
        try {
          for (ConsumerRecord<String, byte[]> r : batch) {
            apply(r);
          }
          if (!batch.isEmpty()) {
            consumer.commitSync();
          }
          backoff = 100;
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          log.warn("revocations not applied, rewinding: {}", e.toString());
          rewind(batch.partitions());
          Thread.sleep(backoff);
          backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
        }
      }
    } catch (WakeupException e) {
      // closing
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      consumer.close(Duration.ofSeconds(2));
    }
  }

  private void apply(ConsumerRecord<String, byte[]> r) {
    if (r.value() == null) {
      return;   // a tombstone lifts a revocation; the removed tokens stay removed
    }
    ControlEvent e;
    try {
      e = ControlEvent.parseFrom(r.value());
    } catch (InvalidProtocolBufferException ex) {
      onMalformed.run();
      log.warn("skipping malformed {}@{}:{}", r.topic(), r.partition(), r.offset());
      return;
    }
    revoker.apply(e);
  }

  private void rewind(Set<TopicPartition> parts) {
    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(parts);
    for (TopicPartition tp : parts) {
      OffsetAndMetadata c = committed.get(tp);
      if (c == null) {
        consumer.seekToBeginning(List.of(tp));
      } else {
        consumer.seek(tp, c.offset());
      }
    }
  }

  @Override
  public synchronized void close() {
    running = false;
    consumer.wakeup();
    if (thread != null) {
      try {
        thread.join(5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
