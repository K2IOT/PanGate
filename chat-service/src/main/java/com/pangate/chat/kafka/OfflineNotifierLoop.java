package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.NotifySink;
import com.pangate.chat.core.OfflineNotifications;
import com.pangate.contracts.v1.OfflineRecord;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
 * CH4 spec §3.1: group {@code chat-notify} on {@code cN.push.offline}. A batch is committed once every request it
 * produced was acked; the store or Kafka failing rewinds it to the last commit after a growing pause (100 ms to 5 s).
 * Requests produced again on a rewind carry the same request id, which notify-service deduplicates.
 */
public final class OfflineNotifierLoop implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(OfflineNotifierLoop.class);
  public static final String GROUP = "chat-notify";
  static final long MAX_BACKOFF_MS = 5_000;

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final OfflineNotifications notifications;
  private final NotifySink sink;
  private final MeterRegistry meters;
  private volatile boolean running = true;
  private Thread thread;

  public OfflineNotifierLoop(Consumer<String, byte[]> consumer, String topic, OfflineNotifications notifications,
      NotifySink sink, MeterRegistry meters) {
    this.consumer = consumer;
    this.topic = topic;
    this.notifications = notifications;
    this.sink = sink;
    this.meters = meters;
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group) {
    return new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ConsumerConfig.GROUP_ID_CONFIG, group,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500), new StringDeserializer(), new ByteArrayDeserializer());
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-notify").start(this::run);
  }

  private void run() {
    long backoff = 100;
    Set<TopicPartition> toRewind = Set.of();
    try {
      consumer.subscribe(List.of(topic));
      while (running) {
        if (!toRewind.isEmpty()) {
          try {
            rewind(toRewind);
            toRewind = Set.of();
          } catch (WakeupException e) {
            throw e;
          } catch (RuntimeException e) {
            // review of CH4: a rewind that times out (coordinator gone) is tried again; it never ends the thread,
            // and nothing is polled past the records still to redo
            log.warn("offline notifier rewind failed, trying again: {}", e.toString());
            Thread.sleep(backoff);
            backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
            continue;
          }
        }
        ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
        if (batch.isEmpty()) {
          continue;
        }
        try {
          step(batch);
          consumer.commitSync();
          backoff = 100;
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          meters.counter("chat.notify.offline.retries").increment();
          log.warn("offline notifications not done, rewinding: {}", e.toString());
          toRewind = batch.partitions();   // rewound at the top of the loop, until it succeeds
          Thread.sleep(backoff);
          backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
        }
      }
    } catch (WakeupException e) {
      // closing
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      log.error("offline notifier stopped", e);
    } finally {
      consumer.close(Duration.ofSeconds(2));
    }
  }

  private void step(ConsumerRecords<String, byte[]> batch) {
    List<CompletableFuture<Void>> sent = new ArrayList<>();
    long now = System.currentTimeMillis();
    for (ConsumerRecord<String, byte[]> r : batch) {
      OfflineRecord off;
      try {
        off = OfflineRecord.parseFrom(r.value() == null ? new byte[0] : r.value());
      } catch (InvalidProtocolBufferException e) {
        meters.counter("chat.notify.offline", "result", "invalid").increment();
        continue;
      }
      OfflineNotifications.Result res = notifications.handle(off, now);
      meters.counter("chat.notify.offline", "result", res.outcome()).increment();
      res.request().ifPresent(q -> sent.add(sink.send(q)));
    }
    CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new)).join();
  }

  private void rewind(Set<TopicPartition> rewound) {
    // a partition lost in a rebalance is not ours to seek; its new owner starts from the last commit
    Set<TopicPartition> parts = Set.copyOf(rewound.stream().filter(consumer.assignment()::contains).toList());
    if (parts.isEmpty()) {
      return;
    }
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
        thread.join(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
