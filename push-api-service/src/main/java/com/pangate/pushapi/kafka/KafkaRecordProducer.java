package com.pangate.pushapi.kafka;

import com.pangate.contracts.testkit.PushHeaders;
import com.pangate.contracts.testkit.PushKeys;
import com.pangate.contracts.testkit.PushTopics;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.pushapi.core.ProduceFailedException;
import com.pangate.pushapi.core.RecordProducer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * {@code cN.push.requests} with the testkit's key and header conventions; each send waits for the ack, at most the
 * produce timeout (push-api spec §2 step 9). The producer gives up on its own after that timeout too, so a record
 * answered 503 is not delivered much later.
 */
public final class KafkaRecordProducer implements RecordProducer, AutoCloseable {

  private final Producer<String, byte[]> producer;
  private final String topic;
  private final String cell;
  private final Duration timeout;

  public KafkaRecordProducer(Producer<String, byte[]> producer, String cell, Duration timeout) {
    this.producer = producer;
    this.topic = PushTopics.requests(cell).name();
    this.cell = cell;
    this.timeout = timeout;
  }

  public static KafkaRecordProducer create(String bootstrap, String cell, Duration timeout) {
    int ms = (int) timeout.toMillis();
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    cfg.put(ProducerConfig.ACKS_CONFIG, "all");
    cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
    cfg.put(ProducerConfig.LINGER_MS_CONFIG, 2);
    cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, ms);
    cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, ms);
    cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ms + 2);
    cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "push-api");
    return new KafkaRecordProducer(new KafkaProducer<>(cfg, new StringSerializer(), new ByteArraySerializer()), cell,
        timeout);
  }

  @Override
  public void send(PushRecord r) {
    ProducerRecord<String, byte[]> pr = new ProducerRecord<>(topic, PushKeys.of(r), r.toByteArray());
    String traceparent = r.hasFrame() ? r.getFrame().getHeadersOrDefault("traceparent", null) : null;
    PushHeaders.write(pr.headers(), r.getTenantId(), cell, traceparent, "PushRecord");
    try {
      long deadline = System.nanoTime() + timeout.toNanos();     // send() may block on metadata: one budget for both
      java.util.concurrent.Future<?> acked = producer.send(pr);
      acked.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ProduceFailedException("interrupted producing " + r.getMsgId(), e);
    } catch (Exception e) {
      throw new ProduceFailedException("produce of " + r.getMsgId() + " failed", e);
    }
  }

  @Override
  public void close() {
    producer.close(Duration.ofSeconds(5));
  }
}
