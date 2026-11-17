package com.pangate.account.infrastructure.kafka;

import com.pangate.account.application.port.Outbox;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends {@code event} outbox rows to the compacted {@code account.users} (key = userId) in id order, one ack at a
 * time (account spec §6). A failure stops the pass; the next pass resends from that row.
 */
public final class EventRelay implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(EventRelay.class);

  private final Outbox outbox;
  private final Producer<String, byte[]> producer;
  private final String topic;
  private final int batch;
  private final Duration timeout;

  public EventRelay(Outbox outbox, Producer<String, byte[]> producer, String topic, int batch, Duration timeout) {
    this.outbox = outbox;
    this.producer = producer;
    this.topic = topic;
    this.batch = batch;
    this.timeout = timeout;
  }

  public static KafkaProducer<String, byte[]> producer(String bootstrap, Duration timeout) {
    int ms = (int) timeout.toMillis();
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    cfg.put(ProducerConfig.ACKS_CONFIG, "all");
    cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, ms);
    cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, ms);
    cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ms + 2);
    cfg.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "account-events");
    return new KafkaProducer<>(cfg, new StringSerializer(), new ByteArraySerializer());
  }

  public int runOnce() {
    int done = 0;
    for (Outbox.OutboxRow r : outbox.pending(Outbox.EVENT, batch)) {
      try {
        producer.send(new ProducerRecord<>(topic, r.key(), r.payload())).get(timeout.toMillis() + 2,
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return done;
      } catch (Exception e) {
        log.debug("event {} not sent yet: {}", r.id(), e.toString());
        return done;
      }
      outbox.markSent(r.id());
      done++;
    }
    return done;
  }

  @Override
  public void close() {
    producer.close(Duration.ofSeconds(2));
  }
}
