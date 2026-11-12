package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.FencedException;
import com.pangate.chat.core.SendResults;
import com.pangate.chat.core.SequencedMessage;
import com.pangate.chat.core.Sequencer;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
 * {@code chat.commands} in group {@code chat-sequencer} (chat spec §4.4). Partitions of a poll run in parallel. Per
 * partition: commands grouped by
 * conversation in offset order → {@link Sequencer#process} → events → HTTP results → commit of that partition. A fence
 * or a store / Kafka failure rewinds the partition to the first record of the poll; the retry finds what was already
 * written through dedup, so no message gets a second seq. A command failing the same deterministic way
 * {@link PoisonGuard#MAX_ATTEMPTS} times in a row is dead-lettered on the last try.
 */
public final class SequencerLoop implements AutoCloseable {

  public static final String GROUP = "chat-sequencer";
  private static final Logger log = LoggerFactory.getLogger(SequencerLoop.class);

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final Sequencer sequencer;
  private final EventProducer events;
  private final SendResults results;
  private final CommandProducer dlq;
  private final MeterRegistry meters;
  private final Timer latency;
  private final Timer lag;
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  private final PoisonGuard poison = new PoisonGuard();
  private volatile boolean running = true;
  private Thread thread;

  public SequencerLoop(Consumer<String, byte[]> consumer, String topic, Sequencer sequencer, EventProducer events,
      SendResults results, CommandProducer dlq, MeterRegistry meters) {
    this.consumer = consumer;
    this.topic = topic;
    this.sequencer = sequencer;
    this.events = events;
    this.results = results;
    this.dlq = dlq;
    this.meters = meters;
    this.latency = meters.timer("chat.send.latency");
    this.lag = meters.timer("chat.sequencer.lag");
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    cfg.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1_000);
    return new KafkaConsumer<>(cfg, new StringDeserializer(), new ByteArrayDeserializer());
  }

  public static ChatEvent event(StoredMessage m, ConversationKind kind) {
    return ChatEvent.newBuilder().setKind(kind).setMessage(ChatMessage.newBuilder().setConversationId(m.convId())
        .setMsgId(m.msgId()).setConvSeq(m.convSeq()).setSenderId(m.senderId()).setClientMsgId(m.clientMsgId())
        .setText(m.text()).setReplyToMsgId(m.replyTo() == null ? "" : m.replyTo())
        .addAllMentionUserIds(m.mentions()).setSentAtMs(m.sentAtMs())).build();
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-sequencer").start(this::run);
  }

  private void run() {
    try {
      consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> parts) {
          sequencer.forget();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> parts) {}
      });
      while (running) {
        try {
          step();
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          log.warn("sequencer: {}; going on", e.toString());
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

  /** Partitions are sequenced in parallel (one virtual thread each); seek and commit stay on the poll thread. */
  private void step() throws InterruptedException {
    ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(200));
    Map<TopicPartition, Future<?>> work = new HashMap<>();
    for (TopicPartition tp : batch.partitions()) {
      List<ConsumerRecord<String, byte[]>> recs = batch.records(tp);
      boolean lastTry = poison.lastTry(tp, recs.getFirst().offset());
      work.put(tp, workers.submit(() -> {
        partition(recs, lastTry);
        return null;
      }));
    }
    Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
    boolean failed = false;
    for (Map.Entry<TopicPartition, Future<?>> w : work.entrySet()) {
      TopicPartition tp = w.getKey();
      List<ConsumerRecord<String, byte[]>> recs = batch.records(tp);
      try {
        w.getValue().get();
        done.put(tp, new OffsetAndMetadata(recs.getLast().offset() + 1));
        poison.clear(tp);
      } catch (ExecutionException e) {
        poison.failed(tp, recs.getFirst().offset(), e.getCause());
        if (e.getCause() instanceof FencedException f) {
          meters.counter("chat.lwt.conflicts").increment();
          log.info("{}: {}; re-reading the partition", tp, f.getMessage());
        } else {
          log.warn("{} not sequenced: {}; retrying", tp, String.valueOf(e.getCause()));
        }
        consumer.seek(tp, recs.getFirst().offset());
        failed = true;
      }
    }
    if (!done.isEmpty()) {
      consumer.commitSync(done);
    }
    if (failed) {
      Thread.sleep(200);
    }
  }

  private record Cmd(ChatCommand command, ConsumerRecord<String, byte[]> record) {}

  /**
   * {@code lastTry}: this poll start already failed {@code MAX_ATTEMPTS - 1} times in a row the same deterministic
   * way; a conversation that fails again is sequenced command by command and the command that fails is dead-lettered.
   */
  private void partition(List<ConsumerRecord<String, byte[]>> recs, boolean lastTry) {
    Map<String, List<Cmd>> byConv = new LinkedHashMap<>();
    long now = System.currentTimeMillis();
    for (ConsumerRecord<String, byte[]> r : recs) {
      lag.record(Math.max(0, now - r.timestamp()), TimeUnit.MILLISECONDS);
      ChatCommand c;
      try {
        c = ChatCommand.parseFrom(r.value() == null ? new byte[0] : r.value());
      } catch (InvalidProtocolBufferException e) {
        c = null;
      }
      if (c == null || !c.hasSend() || c.getSend().getConversationId().isEmpty()) {
        dlq.deadLetter(r.key(), r.value(), "malformed ChatCommand");
        meters.counter("chat.sequencer.malformed").increment();
        continue;
      }
      byConv.computeIfAbsent(c.getSend().getConversationId(), k -> new ArrayList<>()).add(new Cmd(c, r));
    }
    List<ChatEvent> out = new ArrayList<>();
    List<SequencedMessage> replies = new ArrayList<>();
    List<Long> received = new ArrayList<>();
    for (Map.Entry<String, List<Cmd>> e : byConv.entrySet()) {
      List<Cmd> cmds = e.getValue();
      List<SequencedMessage> seq;
      try {
        seq = sequencer.process(e.getKey(), cmds.stream().map(Cmd::command).toList());
      } catch (RuntimeException ex) {
        if (!lastTry || PoisonGuard.isTransient(ex)) {
          throw ex;
        }
        cmds = new ArrayList<>();
        seq = new ArrayList<>();
        for (Cmd c : e.getValue()) {
          try {
            seq.addAll(sequencer.process(e.getKey(), List.of(c.command())));
            cmds.add(c);
          } catch (RuntimeException one) {
            if (PoisonGuard.isTransient(one)) {
              throw one;
            }
            log.error("command at {}@{} failed {} times: {}; dead-lettered", c.record().topic(),
                c.record().offset(), PoisonGuard.MAX_ATTEMPTS, one.toString());
            dlq.deadLetter(c.record().key(), c.record().value(), "poison: " + one);
            meters.counter("chat.sequencer.poison").increment();
          }
        }
      }
      ConversationKind kind = sequencer.kind(e.getKey());
      for (int i = 0; i < seq.size(); i++) {
        SequencedMessage m = seq.get(i);
        out.add(event(m.message(), kind));
        if (m.duplicate()) {
          meters.counter("chat.dedup.hits").increment();
        }
        if (!m.replyTo().isEmpty()) {
          replies.add(m);
        }
        received.add(cmds.get(i).command().getReceivedAtMs());
      }
    }
    events.sendAll(out);
    long at = System.currentTimeMillis();
    received.forEach(t -> latency.record(Math.max(0, at - t), TimeUnit.MILLISECONDS));
    replies.forEach(m -> results.publish(m.replyTo(), m.message()));
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
