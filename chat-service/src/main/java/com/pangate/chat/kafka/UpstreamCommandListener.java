package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.Notifier;
import com.pangate.chat.core.Rejection;
import com.pangate.chat.core.SendInput;
import com.pangate.chat.core.UserIds;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.UpstreamRecord;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The WebSocket send path (chat spec §4.1): {@code cN.gw.upstream.cmd} in group {@code chat-ingest}. Only tenant
 * {@code chat} records with header {@code op=chat.send} are taken; a record that does not parse goes to
 * {@code chat.commands.dlq}; a refused send is told to the sender through {@link Notifier}. Offsets are committed after
 * each poll was handled; when the store or Kafka fails, the partition rewinds to the failed record and is retried.
 */
public final class UpstreamCommandListener implements AutoCloseable {

  public static final String GROUP = "chat-ingest";
  public static final String OP_SEND = "chat.send";
  private static final Logger log = LoggerFactory.getLogger(UpstreamCommandListener.class);

  private final org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer;
  private final String topic;
  private final Ingest ingest;
  private final Notifier notifier;
  private final CommandProducer dlq;
  private final Consumer<String> onOutcome;
  private volatile boolean running = true;
  private Thread thread;

  /** {@code onOutcome}: accepted, rejected, ignored, malformed or error (metric {@code chat.ingest.records}). */
  public UpstreamCommandListener(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic,
      Ingest ingest, Notifier notifier, CommandProducer dlq, Consumer<String> onOutcome) {
    this.consumer = consumer;
    this.topic = topic;
    this.ingest = ingest;
    this.notifier = notifier;
    this.dlq = dlq;
    this.onOutcome = onOutcome;
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    cfg.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return new KafkaConsumer<>(cfg, new StringDeserializer(), new ByteArrayDeserializer());
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-ingest").start(this::run);
  }

  private void run() {
    try {
      consumer.subscribe(List.of(topic));
      while (running) {
        try {
          step();
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          log.warn("ingest listener: {}; going on", e.toString());
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

  private void step() throws InterruptedException {
    ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
    Map<TopicPartition, Long> retryFrom = new HashMap<>();
    for (ConsumerRecord<String, byte[]> r : batch) {
      TopicPartition tp = new TopicPartition(r.topic(), r.partition());
      if (retryFrom.containsKey(tp)) {
        continue;
      }
      try {
        onOutcome.accept(handle(r));
      } catch (RuntimeException e) {
        onOutcome.accept("error");
        log.warn("send at {}@{} not handled: {}", tp, r.offset(), e.toString());
        retryFrom.put(tp, r.offset());
      }
    }
    if (retryFrom.isEmpty()) {
      if (!batch.isEmpty()) {
        consumer.commitSync();
      }
    } else {
      // the other partitions repeat their records too: a repeated send is one message (the sequencer dedups)
      retryFrom.forEach(consumer::seek);
      Thread.sleep(500);
    }
  }

  private String handle(ConsumerRecord<String, byte[]> r) {
    UpstreamRecord u;
    try {
      u = UpstreamRecord.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException e) {
      dlq.deadLetter(r.key(), r.value(), "malformed UpstreamRecord");
      return "malformed";
    }
    if (!ChatTopics.TENANT.equals(u.getTenantId()) || !OP_SEND.equals(u.getHeadersOrDefault("op", ""))) {
      return "ignored";
    }
    ChatSend send;
    try {
      send = ChatSend.parseFrom(u.getPayload());
    } catch (InvalidProtocolBufferException e) {
      dlq.deadLetter(r.key(), r.value(), "malformed ChatSend");
      return "malformed";
    }
    String sender = UserIds.fromPrincipal(u.getPrincipalId()).orElse("");
    Optional<Rejection> rejected = ingest.accept(new SendInput(sender, u.getDeviceId(), send, ""));
    if (rejected.isEmpty()) {
      return "accepted";
    }
    if (!sender.isEmpty()) {
      notifier.rejected(sender, ChatRejected.newBuilder().setConversationId(send.getConversationId())
          .setClientMsgId(send.getClientMsgId()).setReason(rejected.get().reason()).build());
    }
    return "rejected";
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
