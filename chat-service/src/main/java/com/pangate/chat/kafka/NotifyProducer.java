package com.pangate.chat.kafka;

import com.pangate.chat.core.NotifySink;
import com.pangate.contracts.testkit.NotifyKeys;
import com.pangate.contracts.v1.NotifyRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * {@code cN.notify.requests} (CH4 spec §2.1): key = the recipient principal, headers {@code tenant} and
 * {@code content-type}. Idempotent, {@code acks=all}; the future completes on the ack.
 */
public final class NotifyProducer implements NotifySink, AutoCloseable {

  static final String CONTENT_TYPE = "application/x-protobuf; schema=pangate.v1.NotifyRequest";

  private final Producer<String, byte[]> producer;
  private final String topic;
  private final Duration timeout;

  public NotifyProducer(Producer<String, byte[]> producer, String topic, Duration timeout) {
    this.producer = producer;
    this.topic = topic;
    this.timeout = timeout;
  }

  public static KafkaProducer<String, byte[]> producer(String bootstrap) {
    return new KafkaProducer<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
        ProducerConfig.LINGER_MS_CONFIG, 5,
        ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd",
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000,
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000), new StringSerializer(), new ByteArraySerializer());
  }

  @Override
  public CompletableFuture<Void> send(NotifyRequest r) {
    ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, NotifyKeys.of(r), r.toByteArray());
    rec.headers().add("tenant", r.getTenantId().getBytes(StandardCharsets.UTF_8));
    rec.headers().add("content-type", CONTENT_TYPE.getBytes(StandardCharsets.UTF_8));
    CompletableFuture<Void> f = new CompletableFuture<>();
    try {
      producer.send(rec, (meta, e) -> {
        if (e == null) {
          f.complete(null);
        } else {
          f.completeExceptionally(e);
        }
      });
    } catch (RuntimeException e) {
      f.completeExceptionally(e);
    }
    return f.orTimeout(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    producer.close(Duration.ofSeconds(5));
  }
}
