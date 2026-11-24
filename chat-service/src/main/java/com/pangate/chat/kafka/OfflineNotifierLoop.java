package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.NotifySink;
import com.pangate.chat.core.OfflineNotifications;
import com.pangate.chat.core.QueueUnavailableException;
import com.pangate.contracts.v1.OfflineRecord;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * CH4 spec §3.1: group {@code chat-notify} on {@code cN.push.offline}, on a {@link KeyedPipeline} (perf 3c, spec
 * §4.5): one lane per record key (the recipient, or the topic), recipients in parallel. A record is done once its
 * request was acked; the store or Kafka failing retries it in place (100 ms to 5 s). A request produced again carries
 * the same request id, which notify-service deduplicates. There is no DLQ: a record is retried until it gets through.
 */
public final class OfflineNotifierLoop implements AutoCloseable {

  public static final String GROUP = "chat-notify";

  private final OfflineNotifications notifications;
  private final NotifySink sink;
  private final MeterRegistry meters;
  private final KeyedPipeline pipeline;

  public OfflineNotifierLoop(Consumer<String, byte[]> consumer, String topic, OfflineNotifications notifications,
      NotifySink sink, MeterRegistry meters) {
    this(consumer, topic, notifications, sink, meters, KeyedPipeline.Settings.defaults(1, false));
  }

  public OfflineNotifierLoop(Consumer<String, byte[]> consumer, String topic, OfflineNotifications notifications,
      NotifySink sink, MeterRegistry meters, KeyedPipeline.Settings settings) {
    this.notifications = notifications;
    this.sink = sink;
    this.meters = meters;
    this.pipeline = new KeyedPipeline("offline", consumer, topic, settings, (batch, lastTry) -> {
      for (ConsumerRecord<String, byte[]> r : batch) {
        handle(r);
      }
    }, () -> { }, meters);
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group) {
    return new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ConsumerConfig.GROUP_ID_CONFIG, group,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500), new StringDeserializer(), new ByteArrayDeserializer());
  }

  public void start() {
    pipeline.start();
  }

  private void handle(ConsumerRecord<String, byte[]> r) throws InterruptedException {
    OfflineRecord off;
    try {
      off = OfflineRecord.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException e) {
      meters.counter("chat.notify.offline", "result", "invalid").increment();
      return;
    }
    OfflineNotifications.Result res = notifications.handle(off, System.currentTimeMillis());
    if (res.request().isPresent()) {
      try {
        sink.send(res.request().get()).get();
      } catch (ExecutionException e) {
        meters.counter("chat.notify.offline.retries").increment();
        throw new QueueUnavailableException("notify request not acked: " + e.getCause(), e.getCause());
      }
    }
    meters.counter("chat.notify.offline", "result", res.outcome()).increment();
  }

  @Override
  public void close() {
    pipeline.close();
  }
}
