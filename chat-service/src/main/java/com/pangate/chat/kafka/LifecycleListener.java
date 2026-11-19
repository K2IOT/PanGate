package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.PresenceTracker;
import com.pangate.contracts.v1.ConnectionEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CH3 spec §6.1: reads {@code c1.gw.lifecycle} (group {@code chat-presence}) into {@link PresenceTracker}, and ticks
 * it every 500 ms to end offline graces. A record that does not parse is skipped. Review 2026-11-19 (F5): a Redis
 * failure is counted ({@code chat.presence.unavailable}) and the record is read again after a pause (200 ms, doubling
 * to 5 s), never skipped: a lost disconnect would leave a connection that keeps its user online. Presence does not
 * work while Redis is down anyway; once it is back the topic is read on, in order.
 */
public final class LifecycleListener implements AutoCloseable {

  public static final String GROUP = "chat-presence";
  private static final Logger log = LoggerFactory.getLogger(LifecycleListener.class);

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final PresenceTracker tracker;
  private final MeterRegistry meters;
  static final long FIRST_PAUSE_MS = 200;
  static final long MAX_PAUSE_MS = 5_000;

  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(
      Thread.ofPlatform().daemon().name("chat-presence-tick").factory());
  private volatile boolean running = true;
  private Thread thread;

  public LifecycleListener(Consumer<String, byte[]> consumer, String topic, PresenceTracker tracker,
      MeterRegistry meters) {
    this.consumer = consumer;
    this.topic = topic;
    this.tracker = tracker;
    this.meters = meters;
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group) {
    Map<String, Object> c = new HashMap<>();
    c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    c.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    // a new group replays the topic (24 h, in order, deduped): it rebuilds who is connected right now
    c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
    return new KafkaConsumer<>(c, new StringDeserializer(), new ByteArrayDeserializer());
  }

  public synchronized void start() {
    ticker.scheduleWithFixedDelay(() -> {
      try {
        tracker.tick();
      } catch (RuntimeException e) {
        meters.counter("chat.presence.unavailable").increment();
      }
    }, 500, 500, TimeUnit.MILLISECONDS);
    thread = Thread.ofPlatform().daemon().name("chat-presence").start(this::run);
  }

  private void run() {
    try {
      consumer.subscribe(List.of(topic));
      long pause = FIRST_PAUSE_MS;
      while (running) {
        try {
          var batch = consumer.poll(Duration.ofMillis(200));
          Map<org.apache.kafka.common.TopicPartition, Long> next = new HashMap<>();
          boolean failed = false;
          for (ConsumerRecord<String, byte[]> r : batch) {
            org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition(r.topic(),
                r.partition());
            if (failed) {
              next.putIfAbsent(tp, r.offset());   // not handled: read again from here
              continue;
            }
            if (!handle(r)) {
              failed = true;
              next.put(tp, r.offset());
            }
          }
          if (failed) {
            next.forEach(consumer::seek);
          }
          if (!batch.isEmpty()) {
            consumer.commitSync();
          }
          if (failed) {
            Thread.sleep(pause);
            pause = Math.min(MAX_PAUSE_MS, pause * 2);
          } else {
            pause = FIRST_PAUSE_MS;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          // final review I4: a commit failure (rebalance) must not end presence for this pod
          meters.counter("chat.presence.errors").increment();
          log.warn("presence listener step failed: {}", e.toString());
        }
      }
    } catch (WakeupException e) {
      // closing
    } catch (RuntimeException e) {
      log.error("presence listener stopped", e);
    } finally {
      consumer.close(Duration.ofSeconds(5));
    }
  }

  /** False when the store failed and the record is to be read again. */
  private boolean handle(ConsumerRecord<String, byte[]> r) {
    ConnectionEvent e;
    try {
      e = ConnectionEvent.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException ex) {
      meters.counter("chat.presence.records", "result", "malformed").increment();
      return true;
    }
    try {
      tracker.on(e);
      return true;
    } catch (RuntimeException ex) {
      meters.counter("chat.presence.unavailable").increment();
      log.debug("presence event to be read again: {}", ex.toString());
      return false;
    }
  }

  @Override
  public synchronized void close() {
    running = false;
    ticker.shutdownNow();
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
