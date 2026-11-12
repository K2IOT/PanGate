package com.pangate.pushapi.kafka;

import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.pushapi.core.MessageStatus;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code cN.push.offline} and {@code cN.push.delivered} → {@code msg:{t}:<id>} (push-api spec §4.2), in a consumer
 * group; offsets are committed after the Redis writes. When Redis fails, the consumer rewinds to the first record not
 * written and tries again.
 */
public final class StatusListener implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(StatusListener.class);

  private final Consumer<String, byte[]> consumer;
  private final String offlineTopic;
  private final String deliveredTopic;
  private final MessageStatus status;
  private final BiConsumer<String, String> onUpdate;
  private final java.util.function.LongConsumer lagMs;
  private volatile boolean running = true;
  private Thread thread;

  /** {@code onUpdate(topic, result)} for metrics; result is ok, malformed or error. */
  public StatusListener(Consumer<String, byte[]> consumer, String cell, MessageStatus status,
      BiConsumer<String, String> onUpdate) {
    this.consumer = consumer;
    this.offlineTopic = cell + ".push.offline";
    this.deliveredTopic = cell + ".push.delivered";
    this.status = status;
    this.onUpdate = onUpdate;
    this.lagMs = ms -> { };
  }

  /** {@code lagMs} gets, for every outcome written, the time since the router produced it (pushapi.status.lag). */
  public StatusListener(Consumer<String, byte[]> consumer, String cell, MessageStatus status,
      BiConsumer<String, String> onUpdate, java.util.function.LongConsumer lagMs) {
    this.consumer = consumer;
    this.offlineTopic = cell + ".push.offline";
    this.deliveredTopic = cell + ".push.delivered";
    this.status = status;
    this.onUpdate = onUpdate;
    this.lagMs = lagMs;
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("push-status").start(this::run);
  }

  private void run() {
    try {
      consumer.subscribe(List.of(offlineTopic, deliveredTopic));
      while (running) {
        try {
          step();
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          // a commit during a rebalance, a revoked partition, a broker outage: the uncommitted records come again
          log.warn("status listener: {}; going on", e.toString());
          Thread.sleep(1_000);
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

  private void step() throws InterruptedException {
    ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
    Map<TopicPartition, Long> retryFrom = new HashMap<>();
    for (ConsumerRecord<String, byte[]> r : batch) {
      TopicPartition tp = new TopicPartition(r.topic(), r.partition());
      if (retryFrom.containsKey(tp)) {
        continue;
      }
      try {
        long atMs;
        if (r.topic().equals(offlineTopic)) {
          OfflineRecord o = OfflineRecord.parseFrom(r.value());
          status.offline(o);
          atMs = o.getAtMs();
        } else {
          DeliveredRecord d = DeliveredRecord.parseFrom(r.value());
          status.delivered(d);
          atMs = d.getAtMs();
        }
        onUpdate.accept(r.topic(), "ok");
        if (atMs > 0) {
          lagMs.accept(Math.max(0, System.currentTimeMillis() - atMs));
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException | NullPointerException e) {
        onUpdate.accept(r.topic(), "malformed");
      } catch (RuntimeException e) {
        onUpdate.accept(r.topic(), "error");
        retryFrom.put(tp, r.offset());
      }
    }
    if (retryFrom.isEmpty()) {
      if (!batch.isEmpty()) {
        consumer.commitSync();
      }
    } else {
      log.warn("status not written for {} partitions; retrying", retryFrom.size());
      retryFrom.forEach(consumer::seek);
      // commit what was written in the other partitions is not possible per record without bookkeeping;
      // the rewound partitions repeat their records, which the status script tolerates
      Thread.sleep(500);
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
