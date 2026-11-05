package com.pangate.gateway.lifecycle;

import com.pangate.gateway.config.GatewayProperties;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * The gateway's Kafka clients (spec §5.5): a reliable producer (acks=all, idempotent) for cmd, cmd_result and
 * lifecycle, a telemetry producer (acks=1) for everything else, an admin client and the gw.control consumer.
 * {@code max.block.ms=0}: send() never waits on the event loop.
 */
public final class KafkaClients implements AutoCloseable {

  private final GatewayProperties.Kafka cfg;
  private final String nodeId;
  private Admin admin;
  private volatile Producer<String, byte[]> reliable;
  private volatile Producer<String, byte[]> telemetry;

  public KafkaClients(GatewayProperties.Kafka cfg, String nodeId) {
    this.cfg = cfg;
    this.nodeId = nodeId;
  }

  /** Tests: pre-built producers (e.g. MockProducer). */
  KafkaClients(GatewayProperties.Kafka cfg, String nodeId, Producer<String, byte[]> reliable,
      Producer<String, byte[]> telemetry) {
    this(cfg, nodeId);
    this.reliable = reliable;
    this.telemetry = telemetry;
  }

  public String bootstrap() {
    return cfg.bootstrap();
  }

  public synchronized Admin admin() {
    if (admin == null) {
      Map<String, Object> p = new HashMap<>();
      p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
      p.put(AdminClientConfig.CLIENT_ID_CONFIG, "gw-" + nodeId + "-admin");
      p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
      p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
      admin = Admin.create(p);
    }
    return admin;
  }

  public synchronized void startProducers() {
    if (reliable == null) {
      reliable = new KafkaProducer<>(producerProps("reliable", "all", true, cfg.reliableBuffer().toBytes()));
    }
    if (telemetry == null) {
      telemetry = new KafkaProducer<>(producerProps("telemetry", "1", false, cfg.telemetryBuffer().toBytes()));
    }
  }

  Map<String, Object> producerProps(String name, String acks, boolean idempotent, long buffer) {
    Map<String, Object> p = new HashMap<>();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
    p.put(ProducerConfig.CLIENT_ID_CONFIG, "gw-" + nodeId + "-" + name);
    p.put(ProducerConfig.ACKS_CONFIG, acks);
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotent);
    p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, buffer);
    p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    p.put(ProducerConfig.BATCH_SIZE_CONFIG, 65_536);
    p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
    p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
    p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
    // The topic set is small and fixed; with max.block.ms=0 a topic evicted after the default 5 min idle would fail
    // its next send at once. One year, not Long.MAX_VALUE: Kafka adds it to the last access time.
    p.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, java.time.Duration.ofDays(365).toMillis());
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    return p;
  }

  public Producer<String, byte[]> reliable() {
    return require(reliable);
  }

  public Producer<String, byte[]> telemetry() {
    return require(telemetry);
  }

  private static Producer<String, byte[]> require(Producer<String, byte[]> p) {
    if (p == null) {
      throw new IllegalStateException("Kafka producers not started yet");
    }
    return p;
  }

  /** Highest fill ratio of the two producers' buffer pools: 1 - available / total. */
  public double maxBufferRatio() {
    return Math.max(ratio(reliable), ratio(telemetry));
  }

  private static double ratio(Producer<String, byte[]> p) {
    if (p == null) {
      return 0;
    }
    double available = -1;
    double total = -1;
    for (Map.Entry<MetricName, ? extends Metric> e : p.metrics().entrySet()) {
      if (!"producer-metrics".equals(e.getKey().group())) {
        continue;
      }
      if ("buffer-available-bytes".equals(e.getKey().name())) {
        available = ((Number) e.getValue().metricValue()).doubleValue();
      } else if ("buffer-total-bytes".equals(e.getKey().name())) {
        total = ((Number) e.getValue().metricValue()).doubleValue();
      }
    }
    return total <= 0 || available < 0 ? 0 : 1 - available / total;
  }

  public Consumer<String, byte[]> newControlConsumer() {
    Map<String, Object> p = new HashMap<>();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
    p.put(ConsumerConfig.CLIENT_ID_CONFIG, "gw-" + nodeId + "-control");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "gw-" + nodeId);
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return new KafkaConsumer<>(p);
  }

  public void close(Duration timeout) {
    Producer<String, byte[]> r = reliable;
    Producer<String, byte[]> t = telemetry;
    if (r != null) {
      r.close(timeout);
    }
    if (t != null) {
      t.close(timeout);
    }
    synchronized (this) {
      if (admin != null) {
        admin.close(Duration.ofSeconds(2));
        admin = null;
      }
    }
  }

  @Override
  public void close() {
    close(Duration.ofSeconds(10));
  }
}
