package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.ChannelSubscriptions;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.Notifier;
import com.pangate.chat.core.Rejection;
import com.pangate.chat.core.SendInput;
import com.pangate.chat.core.UserIds;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChannelClose;
import com.pangate.contracts.v1.ChannelOpen;
import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.UpstreamRecord;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The WebSocket command path (chat spec §4.1, §6): {@code cN.gw.upstream.cmd} in group {@code chat-ingest}. Only
 * tenant {@code chat} records are taken, with header {@code op} = {@code chat.send}, {@code chat.channel.open} or
 * {@code chat.channel.close}; a record that does not parse goes to
 * {@code chat.commands.dlq}; a refused command is told to the sender through {@link Notifier}. Offsets are committed after
 * each poll was handled; partitions run in parallel; when the store or Kafka fails, the partition rewinds to the
 * failed record and is retried.
 */
public final class UpstreamCommandListener implements AutoCloseable {

  public static final String GROUP = "chat-ingest";
  public static final String OP_SEND = "chat.send";
  public static final String OP_OPEN = "chat.channel.open";
  public static final String OP_CLOSE = "chat.channel.close";
  private static final Logger log = LoggerFactory.getLogger(UpstreamCommandListener.class);

  private final org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer;
  private final String topic;
  private final Ingest ingest;
  private final Notifier notifier;
  private final CommandProducer dlq;
  private final Consumer<String> onOutcome;
  private final ChannelSubscriptions channels;
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  private volatile boolean running = true;
  private Thread thread;

  /** {@code onOutcome}: accepted, rejected, ignored, malformed or error (metric {@code chat.ingest.records}). */
  public UpstreamCommandListener(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic,
      Ingest ingest, Notifier notifier, CommandProducer dlq, Consumer<String> onOutcome,
      ChannelSubscriptions channels) {
    this.consumer = consumer;
    this.topic = topic;
    this.ingest = ingest;
    this.notifier = notifier;
    this.dlq = dlq;
    this.onOutcome = onOutcome;
    this.channels = channels;
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
      workers.shutdownNow();
      consumer.close(Duration.ofSeconds(2));
    }
  }

  /** Partitions run in parallel (one virtual thread each), records in order; seek and commit on the poll thread. */
  private void step() throws InterruptedException {
    ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
    Map<TopicPartition, Future<Long>> work = new HashMap<>();
    for (TopicPartition tp : batch.partitions()) {
      List<ConsumerRecord<String, byte[]>> recs = batch.records(tp);
      work.put(tp, workers.submit(() -> handleAll(tp, recs)));
    }
    Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
    boolean failed = false;
    for (Map.Entry<TopicPartition, Future<Long>> w : work.entrySet()) {
      TopicPartition tp = w.getKey();
      List<ConsumerRecord<String, byte[]>> recs = batch.records(tp);
      long stop;
      try {
        stop = w.getValue().get();
      } catch (ExecutionException e) {
        stop = recs.getFirst().offset();
      }
      if (stop < 0) {
        done.put(tp, new OffsetAndMetadata(recs.getLast().offset() + 1));
      } else {
        // a repeated send is one message (the sequencer dedups), so re-reading from here is safe
        if (stop > recs.getFirst().offset()) {
          done.put(tp, new OffsetAndMetadata(stop));
        }
        consumer.seek(tp, stop);
        failed = true;
      }
    }
    if (!done.isEmpty()) {
      consumer.commitSync(done);
    }
    if (failed) {
      Thread.sleep(500);
    }
  }

  /** -1 when every record was handled, else the offset to retry from. */
  private long handleAll(TopicPartition tp, List<ConsumerRecord<String, byte[]>> recs) {
    for (ConsumerRecord<String, byte[]> r : recs) {
      try {
        onOutcome.accept(handle(r));
      } catch (RuntimeException e) {
        onOutcome.accept("error");
        log.warn("command at {}@{} not handled: {}", tp, r.offset(), e.toString());
        return r.offset();
      }
    }
    return -1;
  }

  private String handle(ConsumerRecord<String, byte[]> r) {
    UpstreamRecord u;
    try {
      u = UpstreamRecord.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException e) {
      dlq.deadLetter(r.key(), r.value(), "malformed UpstreamRecord");
      return "malformed";
    }
    String op = u.getHeadersOrDefault("op", "");
    if (!ChatTopics.TENANT.equals(u.getTenantId())) {
      return "ignored";
    }
    if (op.equals(OP_OPEN) || op.equals(OP_CLOSE)) {
      return channel(r, u, op);
    }
    if (!OP_SEND.equals(op)) {
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

  private String channel(ConsumerRecord<String, byte[]> r, UpstreamRecord u, String op) {
    String convId;
    try {
      convId = op.equals(OP_OPEN) ? ChannelOpen.parseFrom(u.getPayload()).getConversationId()
          : ChannelClose.parseFrom(u.getPayload()).getConversationId();
    } catch (InvalidProtocolBufferException e) {
      dlq.deadLetter(r.key(), r.value(), "malformed " + op);
      return "malformed";
    }
    Optional<String> user = UserIds.fromPrincipal(u.getPrincipalId());
    if (user.isEmpty()) {
      return "rejected";
    }
    Optional<Rejection> rejected = op.equals(OP_OPEN) ? channels.open(user.get(), convId)
        : channels.close(user.get(), convId);
    if (rejected.isEmpty()) {
      return "accepted";
    }
    notifier.rejected(user.get(), ChatRejected.newBuilder().setConversationId(convId)
        .setReason(rejected.get().reason()).build());
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
