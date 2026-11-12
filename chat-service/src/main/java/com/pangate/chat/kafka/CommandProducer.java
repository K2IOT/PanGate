package com.pangate.chat.kafka;

import com.pangate.chat.core.CommandSink;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatCommand;
import java.nio.charset.StandardCharsets;
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
 * {@code chat.commands} keyed by conversation id (chat spec §2), and {@code chat.commands.dlq} for records that cannot
 * be parsed (§8). Each send waits for the ack, at most the produce timeout; a failure throws
 * {@link IllegalStateException} so the caller answers 503 or retries.
 */
public final class CommandProducer implements CommandSink, AutoCloseable {

  private final Producer<String, byte[]> producer;
  private final Duration timeout;
  private final String commandsTopic;

  public CommandProducer(Producer<String, byte[]> producer, Duration timeout, String commandsTopic) {
    this.producer = producer;
    this.timeout = timeout;
    this.commandsTopic = commandsTopic;
  }

  public static CommandProducer create(String bootstrap, Duration timeout) {
    return create(bootstrap, timeout, ChatTopics.COMMANDS);
  }

  public static CommandProducer create(String bootstrap, Duration timeout, String commandsTopic) {
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
    cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "chat-ingest");
    return new CommandProducer(new KafkaProducer<>(cfg, new StringSerializer(), new ByteArraySerializer()), timeout,
        commandsTopic);
  }

  @Override
  public void publish(ChatCommand c) {
    send(new ProducerRecord<>(commandsTopic, c.getSend().getConversationId(), c.toByteArray()));
  }

  /** The original record, with a {@code reason} header. */
  public void deadLetter(String key, byte[] value, String reason) {
    ProducerRecord<String, byte[]> r = new ProducerRecord<>(ChatTopics.DLQ, key, value);
    r.headers().add("reason", reason.getBytes(StandardCharsets.UTF_8));
    send(r);
  }

  private void send(ProducerRecord<String, byte[]> r) {
    try {
      long deadline = System.nanoTime() + timeout.toNanos();
      producer.send(r).get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted producing to " + r.topic(), e);
    } catch (Exception e) {
      throw new IllegalStateException("produce to " + r.topic() + " failed", e);
    }
  }

  @Override
  public void close() {
    producer.close(Duration.ofSeconds(5));
  }
}
