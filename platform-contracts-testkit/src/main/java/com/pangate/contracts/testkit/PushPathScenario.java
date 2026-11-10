package com.pangate.contracts.testkit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.EventType;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The push path on a real Kafka (push contracts spec §6). One container per JVM, topics of {@link PushTopics#all}; every
 * record an {@code await…} returns has been checked against the key and header conventions.
 *
 * <pre>{@code @RegisterExtension static PushPathScenario scenario = new PushPathScenario("c1");}</pre>
 */
public final class PushPathScenario implements BeforeAllCallback, AfterAllCallback {

  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  private static KafkaContainer kafka;

  private final String cell;
  private KafkaProducer<String, byte[]> producer;

  public PushPathScenario(String cell) {
    this.cell = cell;
  }

  private static synchronized KafkaContainer kafka() {
    if (kafka == null) {
      kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
      kafka.start();
    }
    return kafka;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
      for (PushTopics.Spec s : PushTopics.all(cell)) {
        NewTopic t = new NewTopic(s.name(), s.partitions(), (short) 1)
            .configs(Map.of("retention.ms", Long.toString(s.retention().toMillis())));
        try {
          admin.createTopics(List.of(t)).all().get();
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof TopicExistsException)) {
            throw e;
          }
        }
      }
    }
    producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
        ProducerConfig.ACKS_CONFIG, "all", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
        new StringSerializer(), new ByteArraySerializer());
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (producer != null) {
      producer.close(Duration.ofSeconds(5));
    }
  }

  public String bootstrapServers() {
    return kafka().getBootstrapServers();
  }

  public String cell() {
    return cell;
  }

  /**
   * As Push API does: checks {@link PushRecordRules}, key {@link PushKeys#of(PushRecord)}, headers per convention, waits
   * for the ack. Use {@link #produceRaw} to send a record that breaks the rules on purpose.
   */
  public void produce(PushRecord r) {
    PushRecordRules.assertValid(r);
    produce(PushTopics.requests(cell).name(), PushKeys.of(r), r, "PushRecord", r.getTenantId());
  }

  /** Any push-path message with the conventional headers. */
  public void produce(String topic, String key, Message value, String messageName, String tenant) {
    ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, key, value.toByteArray());
    PushHeaders.write(rec.headers(), tenant, cell, null, messageName);
    send(rec);
  }

  /** Exactly these bytes and headers, e.g. to prove a convention check fails. */
  public void produceRaw(String topic, String key, byte[] value, Headers headers) {
    ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, null, key, value, headers);
    send(rec);
  }

  private void send(ProducerRecord<String, byte[]> rec) {
    try {
      producer.send(rec).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("produce to " + rec.topic() + " failed", e.getCause());
    }
  }

  public PushRecord awaitRequest(String msgId) {
    return awaitRequest(msgId, DEFAULT_TIMEOUT);
  }

  public PushRecord awaitRequest(String msgId, Duration timeout) {
    return await(PushTopics.requests(cell).name(), "PushRecord", PushRecord::parseFrom,
        r -> r.getMsgId().equals(msgId), PushKeys::of, timeout, "PushRecord " + msgId);
  }

  public OfflineRecord awaitOffline(String msgId, OfflineRecord.Reason reason) {
    return awaitOffline(msgId, reason, DEFAULT_TIMEOUT);
  }

  public OfflineRecord awaitOffline(String msgId, OfflineRecord.Reason reason, Duration timeout) {
    return await(PushTopics.offline(cell).name(), "OfflineRecord", OfflineRecord::parseFrom,
        r -> r.getRecord().getMsgId().equals(msgId) && r.getReason() == reason, PushKeys::of, timeout,
        "OfflineRecord " + msgId + " " + reason);
  }

  public DeliveredRecord awaitDelivered(String msgId) {
    return awaitDelivered(msgId, DEFAULT_TIMEOUT);
  }

  public DeliveredRecord awaitDelivered(String msgId, Duration timeout) {
    return await(PushTopics.delivered(cell).name(), "DeliveredRecord", DeliveredRecord::parseFrom,
        r -> r.getMsgId().equals(msgId), PushKeys::of, timeout, "DeliveredRecord " + msgId);
  }

  public ConnectionEvent awaitNodeDown(String nodeId, long epoch) {
    return awaitNodeDown(nodeId, epoch, DEFAULT_TIMEOUT);
  }

  public ConnectionEvent awaitNodeDown(String nodeId, long epoch, Duration timeout) {
    return await(PushTopics.lifecycle(cell).name(), "ConnectionEvent", ConnectionEvent::parseFrom,
        e -> e.getType() == EventType.EVENT_TYPE_NODE_DOWN && e.getNodeId().equals(nodeId) && e.getEpoch() == epoch,
        PushKeys::of, timeout, "NODE_DOWN " + nodeId + ":" + epoch);
  }

  /** Every record currently in {@code topic}, in partition then offset order. */
  public List<ConsumerRecord<String, byte[]>> drain(String topic) {
    List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
    try (var c = consumer()) {
      List<TopicPartition> parts = assignAll(c, topic);
      Map<TopicPartition, Long> end = c.endOffsets(parts);
      long deadline = System.nanoTime() + DEFAULT_TIMEOUT.toNanos();
      while (parts.stream().anyMatch(tp -> c.position(tp) < end.get(tp)) && System.nanoTime() < deadline) {
        c.poll(Duration.ofMillis(200)).forEach(out::add);
      }
    }
    return out;
  }

  @FunctionalInterface
  private interface Parser<T> {
    T parse(byte[] b) throws InvalidProtocolBufferException;
  }

  private <T extends Message> T await(String topic, String messageName, Parser<T> parser, Predicate<T> match,
      Function<T, String> key, Duration timeout, String what) {
    try (var c = consumer()) {
      assignAll(c, topic);
      long deadline = System.nanoTime() + timeout.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<String, byte[]> r : c.poll(Duration.ofMillis(200))) {
          Optional<T> v = parse(parser, r.value());
          if (v.isPresent() && match.test(v.get())) {
            PushHeaders.assertConventions(r, messageName);
            if (!key.apply(v.get()).equals(r.key())) {
              throw new AssertionError(what + " has key " + r.key() + ", expected " + key.apply(v.get()));
            }
            return v.get();
          }
        }
      }
    }
    throw new AssertionError(what + " not seen on " + topic + " within " + timeout);
  }

  private static <T> Optional<T> parse(Parser<T> parser, byte[] value) {
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(parser.parse(value));
    } catch (InvalidProtocolBufferException e) {
      return Optional.empty();
    }
  }

  private KafkaConsumer<String, byte[]> consumer() {
    return new KafkaConsumer<>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new ByteArrayDeserializer());
  }

  private static List<TopicPartition> assignAll(KafkaConsumer<String, byte[]> c, String topic) {
    List<TopicPartition> parts = c.partitionsFor(topic, Duration.ofSeconds(10)).stream()
        .map(p -> new TopicPartition(topic, p.partition())).toList();
    c.assign(parts);
    c.seekToBeginning(parts);
    return parts;
  }
}
