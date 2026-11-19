package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.ChatStores.Marks;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.core.PrivacyCache;
import com.pangate.chat.core.Pusher;
import com.pangate.chat.core.ReceiptAggregator;
import com.pangate.chat.core.ReceiptRecipients;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.ReceiptCommand;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CH3 spec §3.4: reads {@code chat.receipts} (group {@code chat-receipts}), so every mark of a conversation is handled
 * in one place. Marks are merged per conversation window, written to ScyllaDB (never back, {@code USING TIMESTAMP}),
 * then pushed. An offset is committed only once every mark up to it is written; a store failure clears the window
 * and rewinds to the last commit (replayed marks are harmless). Pushes are best effort: 3 tries, then dropped.
 */
public final class ReceiptWorkerLoop implements AutoCloseable {

  public static final String GROUP = "chat-receipts";
  static final int PUSH_TRIES = 3;
  static final Duration ROUND_DEADLINE = Duration.ofSeconds(3);
  /** Codex review of PR #6: at most this long is waited for a Push API {@code retry_after_ms} between rounds. */
  static final long MAX_RETRY_AFTER_MS = 2_000;
  static final long MAX_BACKOFF_MS = 5_000;
  static final int MAX_KINDS = 100_000;

  private static final Logger log = LoggerFactory.getLogger(ReceiptWorkerLoop.class);

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final ChatStores.ConversationStore conversations;
  private final MemberLookup members;
  private final ChatStores.MarkStore marks;
  private final PrivacyCache privacy;
  private final Pusher pusher;
  private final ChatProperties.State cfg;
  private final MeterRegistry meters;
  private final ReceiptAggregator agg;
  private final Map<String, ConversationKind> kinds = new ConcurrentHashMap<>();
  /** conversation → (partition, offset of its first unwritten mark). */
  private final Map<String, Map.Entry<TopicPartition, Long>> firstPending = new HashMap<>();
  /** partition → next offset after the last record taken. */
  private final Map<TopicPartition, Long> next = new HashMap<>();
  private final Map<TopicPartition, Long> committed = new HashMap<>();
  private volatile boolean running = true;
  private Thread thread;
  private long backoff = 100;

  public ReceiptWorkerLoop(Consumer<String, byte[]> consumer, String topic,
      ChatStores.ConversationStore conversations, MemberLookup members, ChatStores.MarkStore marks,
      PrivacyCache privacy, Pusher pusher, ChatProperties.State cfg, MeterRegistry meters) {
    this.consumer = consumer;
    this.topic = topic;
    this.conversations = conversations;
    this.members = members;
    this.marks = marks;
    this.privacy = privacy;
    this.pusher = pusher;
    this.cfg = cfg;
    this.meters = meters;
    this.agg = new ReceiptAggregator(() -> System.nanoTime() / 1_000_000, cfg.dmWindow(), cfg.groupWindow());
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group) {
    Map<String, Object> c = new HashMap<>();
    c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    c.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
    return new KafkaConsumer<>(c, new StringDeserializer(), new ByteArrayDeserializer());
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-receipts").start(this::run);
  }

  private void run() {
    try {
      consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> parts) {
          reset();   // the next owner replays from the last commit
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> parts) {}
      });
      while (running) {
        boolean ok;
        try {
          for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(100))) {
            take(r);
          }
          ok = flushDue();
          if (ok) {
            commit();
            backoff = 100;
          }
        } catch (WakeupException e) {
          throw e;
        } catch (com.pangate.chat.core.StoreUnavailableException e) {
          meters.counter("chat.receipts.store.failures").increment();
          log.warn("receipt worker: store unavailable: {}", e.toString());
          ok = false;
        } catch (RuntimeException e) {
          // final review I4: a commit failure (rebalance) or anything unexpected rewinds; it never ends the thread
          meters.counter("chat.receipts.errors").increment();
          log.warn("receipt worker step failed: {}", e.toString());
          ok = false;
        }
        if (!ok) {
          try {
            rewind();
          } catch (WakeupException e) {
            throw e;
          } catch (RuntimeException e) {
            log.warn("receipt worker rewind failed: {}", e.toString());
            reset();
            Thread.sleep(backoff);
          }
        }
      }
    } catch (WakeupException e) {
      // closing
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      log.error("receipt worker stopped", e);
    } finally {
      consumer.close(Duration.ofSeconds(5));
    }
  }

  private void take(ConsumerRecord<String, byte[]> r) {
    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
    next.put(tp, r.offset() + 1);
    ReceiptCommand c;
    try {
      c = ReceiptCommand.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException e) {
      meters.counter("chat.receipts.records", "result", "malformed").increment();
      return;
    }
    Optional<ConversationKind> kind = kind(c.getConversationId());
    if (kind.isEmpty()) {
      meters.counter("chat.receipts.records", "result", "unknown_conversation").increment();
      return;
    }
    firstPending.putIfAbsent(c.getConversationId(), Map.entry(tp, r.offset()));
    agg.add(c, kind.get());
  }

  private Optional<ConversationKind> kind(String convId) {
    ConversationKind k = kinds.get(convId);
    if (k != null) {
      return Optional.of(k);
    }
    Optional<ChatStores.Conversation> c = conversations.get(convId);
    c.ifPresent(x -> {
      if (kinds.size() >= MAX_KINDS) {
        kinds.clear();
      }
      kinds.put(convId, x.kind());
    });
    return c.map(ChatStores.Conversation::kind);
  }

  /**
   * False when the store failed: the caller rewinds. Review of PR #6: every due conversation's marks are written
   * first and nothing is pushed unless all of them were, so a rewind never repeats a push; then all pushes of the step
   * go out together, one set of rounds per step however many conversations were due.
   */
  private boolean flushDue() {
    List<PushRequest> pushes = new java.util.ArrayList<>();
    List<String> flushed = new java.util.ArrayList<>();
    for (ReceiptAggregator.Flush f : agg.due()) {
      List<Member> to;
      try {
        to = com.pangate.chat.core.ReceiptRecipients.audience(f, members);   // perf 3a: no channel list
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Marks> e : f.byUser().entrySet()) {
          marks.advance(f.convId(), e.getKey(), e.getValue().deliveredSeq(), e.getValue().readSeq(),
              f.deliveredOnly().getOrDefault(e.getKey(), 0L), now);
        }
        if (f.kind() == ConversationKind.CONVERSATION_KIND_DIRECT) {
          for (Member m : to) {
            if (!f.byUser().containsKey(m.userId()) && ReceiptRecipients.peerSeesChange(f, m.userId(), privacy::of)) {
              marks.touchState(m.userId(), f.convId(), now);   // the peer's /v1/state shows the change (§7.2)
            }
          }
        }
      } catch (RuntimeException e) {
        meters.counter("chat.receipts.store.failures").increment();
        log.warn("receipt marks of {} not written: {}", f.convId(), e.toString());
        return false;
      }
      flushed.add(f.convId());
      pushes.addAll(ReceiptRecipients.plan(f, to, privacy::of, cfg.listedReceiptsMax()));
    }
    flushed.forEach(firstPending::remove);
    push(pushes);
    return true;
  }

  /** Final review I4: every push of a flush at once, one deadline per round, at most {@link #PUSH_TRIES} rounds. */
  private void push(List<PushRequest> requests) {
    List<PushRequest> pending = requests;
    for (int round = 0; round < PUSH_TRIES && running && !pending.isEmpty(); round++) {
      List<java.util.concurrent.CompletableFuture<PushResponse>> sent = new java.util.ArrayList<>(pending.size());
      for (PushRequest r : pending) {
        try {
          sent.add(pusher.send(r));
        } catch (RuntimeException e) {
          sent.add(java.util.concurrent.CompletableFuture.failedFuture(e));
        }
      }
      long deadline = System.nanoTime() + ROUND_DEADLINE.toNanos();
      List<PushRequest> retry = new java.util.ArrayList<>();
      long retryAfter = 0;
      for (int i = 0; i < pending.size(); i++) {
        boolean ok = false;
        try {
          PushResponse res = sent.get(i).get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
          String code = res.getCode().name();
          ok = !code.endsWith("UNAVAILABLE") && !code.endsWith("RATE_LIMITED");
          if (!ok) {
            retryAfter = Math.max(retryAfter, res.getRetryAfterMs());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          // timed out or failed: retried next round
        }
        if (ok) {
          meters.counter("chat.receipts.pushes", "result", "ok").increment();
        } else {
          retry.add(pending.get(i));
        }
      }
      pending = retry;
      if (!pending.isEmpty() && retryAfter > 0 && round + 1 < PUSH_TRIES) {
        try {
          Thread.sleep(Math.min(MAX_RETRY_AFTER_MS, retryAfter));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    meters.counter("chat.receipts.pushes", "result", "dropped").increment(pending.size());
  }

  private void commit() {
    Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> n : next.entrySet()) {
      long safe = n.getValue();
      for (Map.Entry<TopicPartition, Long> p : firstPending.values()) {
        if (p.getKey().equals(n.getKey())) {
          safe = Math.min(safe, p.getValue());
        }
      }
      if (!Long.valueOf(safe).equals(committed.get(n.getKey()))) {
        out.put(n.getKey(), new OffsetAndMetadata(safe));
      }
    }
    if (!out.isEmpty()) {
      consumer.commitSync(out);
      out.forEach((tp, o) -> committed.put(tp, o.offset()));
    }
  }

  /** A store failure: drop the windows and read again from the last commit, after a backoff. */
  private void rewind() throws InterruptedException {
    reset();
    for (TopicPartition tp : consumer.assignment()) {
      OffsetAndMetadata c = consumer.committed(java.util.Set.of(tp)).get(tp);
      if (c == null) {
        consumer.seekToBeginning(List.of(tp));
      } else {
        consumer.seek(tp, c.offset());
      }
    }
    Thread.sleep(backoff);
    backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
  }

  private void reset() {
    agg.clear();
    firstPending.clear();
    next.clear();
    committed.clear();
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
