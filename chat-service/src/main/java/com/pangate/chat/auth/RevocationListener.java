package com.pangate.chat.auth;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replays {@code gw.control} from the beginning without a consumer group, then follows it, keeping the connect-token
 * revocations ({@link Revocations}; chat spec §1).
 * {@link #caughtUp()} turns true once every partition reached the end offset it had when the replay started.
 */
public final class RevocationListener implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RevocationListener.class);

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final Revocations state;
  private final Runnable onMalformed;
  private volatile boolean caughtUp;
  private volatile boolean running = true;
  private Thread thread;

  public RevocationListener(Consumer<String, byte[]> consumer, String topic, Revocations state, Runnable onMalformed) {
    this.consumer = consumer;
    this.topic = topic;
    this.state = state;
    this.onMalformed = onMalformed;
  }

  public boolean caughtUp() {
    return caughtUp;
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-revocations").start(this::run);
  }

  private void run() {
    try {
      List<TopicPartition> parts = List.of();
      Map<TopicPartition, Long> end = null;
      while (running) {
        try {
          if (end == null) {
            parts = consumer.partitionsFor(topic, Duration.ofSeconds(5)).stream()
                .map(p -> new TopicPartition(topic, p.partition())).toList();
            if (parts.isEmpty()) {
              log.warn("{} has no partitions yet; retrying", topic);
              Thread.sleep(1_000);
              continue;
            }
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            end = consumer.endOffsets(parts);
          }
          for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(200))) {
            try {
              state.apply(r.key(), r.value());
            } catch (IllegalArgumentException e) {
              onMalformed.run();
              log.warn("skipping malformed {}@{}:{}", r.topic(), r.partition(), r.offset());
            }
          }
          Map<TopicPartition, Long> replayTo = end;
          if (!caughtUp && replayTo.entrySet().stream().allMatch(e -> consumer.position(e.getKey()) >= e.getValue())) {
            caughtUp = true;
            log.info("{} replayed ({} partitions)", topic, parts.size());
          }
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          // a broker outage must not stop revocations and kill switches for good: the position is kept
          log.warn("{} listener: {}; going on", topic, e.toString());
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
