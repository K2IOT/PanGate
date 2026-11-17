package com.pangate.notify.infrastructure.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.notify.application.NotifyDispatcher;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;
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
 * Offline notifications spec §5.4: group {@code notify} on {@code cN.notify.requests}. A polled batch is dispatched
 * at once, at most {@code maxInFlight} requests in the air, and committed when every request of it is done. A
 * dispatch failing (the store down) rewinds the batch to the last commit after a growing pause; requests already sent
 * are then deduplicated (§5.4).
 */
public final class NotifyRequestLoop implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(NotifyRequestLoop.class);
  public static final String GROUP = "notify";
  static final long MAX_BACKOFF_MS = 5_000;

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final NotifyDispatcher dispatcher;
  private final Semaphore inFlight;
  private final MeterRegistry meters;
  private final LongSupplier wallMs;
  private volatile boolean running = true;
  private Thread thread;

  public NotifyRequestLoop(Consumer<String, byte[]> consumer, String topic, NotifyDispatcher dispatcher,
      int maxInFlight, MeterRegistry meters, LongSupplier wallMs) {
    this.consumer = consumer;
    this.topic = topic;
    this.dispatcher = dispatcher;
    this.inFlight = new Semaphore(maxInFlight);
    this.meters = meters;
    this.wallMs = wallMs;
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
    thread = Thread.ofPlatform().daemon().name("notify-requests").start(this::run);
  }

  private void run() {
    long backoff = 100;
    try {
      consumer.subscribe(List.of(topic));
      while (running) {
        ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
        if (batch.isEmpty()) {
          continue;
        }
        try {
          dispatch(batch);
          consumer.commitSync();
          backoff = 100;
        } catch (WakeupException e) {
          throw e;
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          meters.counter("notify.requests.retries").increment();
          log.warn("notify batch not done, rewinding: {}", e.toString());
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

  private void dispatch(ConsumerRecords<String, byte[]> batch) throws Exception {
    List<CompletableFuture<String>> all = new ArrayList<>();
    for (ConsumerRecord<String, byte[]> r : batch) {
      NotifyRequest req;
      try {
        req = NotifyRequest.parseFrom(r.value() == null ? new byte[0] : r.value());
      } catch (InvalidProtocolBufferException e) {
        meters.counter("notify.requests", "result", "invalid").increment();
        continue;
      }
      inFlight.acquire();
      CompletableFuture<String> f;
      try {
        f = dispatcher.handle(req, wallMs.getAsLong());
      } catch (RuntimeException e) {
        f = CompletableFuture.failedFuture(e);
      }
      all.add(f.whenComplete((v, e) -> inFlight.release()));
    }
    try {
      CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).join();
    } catch (java.util.concurrent.CompletionException e) {
      throw e.getCause() instanceof Exception ex ? ex : e;
    }
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
        thread.join(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
