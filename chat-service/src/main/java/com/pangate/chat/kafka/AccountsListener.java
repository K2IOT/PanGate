package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.ChatRefusal;
import com.pangate.chat.core.ChatStores;
import com.pangate.contracts.v1.AccountState;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * {@code account.users} in group {@code chat-accounts} (account spec §8): the compacted current state of every
 * account, read from the start, so chat-service can always rebuild its {@code users} table. A state older than the
 * stored one is skipped, and {@code deleted} is final. The first time a user turns {@code deleted}, {@code onDeleted}
 * runs (leave groups and channels) before the state is stored, so a transient failure there retries it; a refusal
 * ({@link ChatRefusal}) is deterministic and is logged, not retried. A failing record rewinds its partition.
 */
public final class AccountsListener implements AutoCloseable {

  public static final String GROUP = "chat-accounts";
  private static final Logger log = LoggerFactory.getLogger(AccountsListener.class);

  private final org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer;
  private final String topic;
  private final ChatStores.UserStore users;
  private final Consumer<String> onDeleted;
  private volatile boolean running = true;
  private Thread thread;

  public AccountsListener(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic,
      ChatStores.UserStore users, Consumer<String> onDeleted) {
    this.consumer = consumer;
    this.topic = topic;
    this.users = users;
    this.onDeleted = onDeleted;
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    cfg.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return new KafkaConsumer<>(cfg, new StringDeserializer(), new ByteArrayDeserializer());
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-accounts").start(this::run);
  }

  private void run() {
    try {
      consumer.subscribe(List.of(topic));
      while (running) {
        Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
        boolean failed = false;
        var batch = consumer.poll(Duration.ofMillis(500));
        for (TopicPartition tp : batch.partitions()) {
          for (ConsumerRecord<String, byte[]> r : batch.records(tp)) {
            try {
              handle(r);
              done.put(tp, new OffsetAndMetadata(r.offset() + 1));
            } catch (RuntimeException e) {
              log.warn("account state {}@{} not applied: {}; retrying", tp, r.offset(), e.toString());
              consumer.seek(tp, r.offset());
              failed = true;
              break;
            }
          }
        }
        if (!done.isEmpty()) {
          consumer.commitSync(done);
        }
        if (failed) {
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

  void handle(ConsumerRecord<String, byte[]> r) {
    AccountState s;
    try {
      s = AccountState.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException e) {
      log.warn("malformed account state at offset {}: skipped", r.offset());
      return;
    }
    if (s.getUserId().isEmpty()) {
      return;
    }
    String status = s.getStatus() == AccountState.Status.STATUS_DELETED ? "deleted" : "active";
    Optional<ChatStores.UserRecord> prior = users.user(s.getUserId());
    // deleted is final: a later-stamped state (an admin write racing the deletion, a skewed clock) never revives it
    if (prior.isPresent() && (prior.get().status().equals("deleted")
        || prior.get().updatedAtMs() > s.getUpdatedAtMs())) {
      return;
    }
    if (status.equals("deleted")) {
      try {
        onDeleted.accept(s.getUserId());
      } catch (ChatRefusal e) {
        // deterministic: retrying would hold this partition (and every later account on it) forever
        log.error("removal of deleted account {} refused: {}; recorded as deleted anyway", s.getUserId(),
            e.getMessage());
      }
    }
    users.upsertIfNewer(s.getUserId(), status, s.getUpdatedAtMs());
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
