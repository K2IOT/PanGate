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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The WebSocket command path (chat spec §4.1, §6): {@code cN.gw.upstream.cmd}, on a {@link KeyedPipeline} keyed by
 * the sender's principal (perf 3c, spec §4.4). Only tenant {@code chat} records are taken. Two roles, each its own
 * consumer group: {@link Role#INGEST} (group {@code chat-ingest}) takes {@code chat.send}, {@code chat.channel.open} and
 * {@code chat.channel.close}; {@link Role#STATE} (group {@code chat-state}) takes {@code chat.delivered},
 * {@code chat.read}, {@code chat.typing} and {@code chat.presence.watch}, so a slow watch never sits in front of a
 * message. The other role's ops are {@code ignored}. A record of a {@code chat:} principal (the record key) that does
 * not parse goes to {@code chat.commands.dlq}; a refused command is told to the sender through {@link Notifier}. A
 * failing record is retried in place; one failing the same deterministic way {@link PoisonGuard#MAX_ATTEMPTS} times
 * in a row is dead-lettered.
 */
public final class UpstreamCommandListener implements AutoCloseable {

  /** Perf 3c (spec §4.4): which ops this listener takes. */
  public enum Role { INGEST, STATE }

  public static final String GROUP = "chat-ingest";
  public static final String STATE_GROUP = "chat-state";
  public static final String OP_SEND = "chat.send";
  public static final String OP_OPEN = "chat.channel.open";
  public static final String OP_CLOSE = "chat.channel.close";
  private static final Logger log = LoggerFactory.getLogger(UpstreamCommandListener.class);

  private final Ingest ingest;
  private final Notifier notifier;
  private final CommandProducer dlq;
  private final Consumer<String> onOutcome;
  private final ChannelSubscriptions channels;
  private final com.pangate.chat.core.StateCommands state;
  private final Role role;
  private final KeyedPipeline pipeline;

  /** {@code onOutcome}: accepted, rejected, ignored, malformed, poison or error (metric {@code chat.ingest.records}). */
  public UpstreamCommandListener(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic,
      Ingest ingest, Notifier notifier, CommandProducer dlq, Consumer<String> onOutcome,
      ChannelSubscriptions channels) {
    this(consumer, topic, ingest, notifier, dlq, onOutcome, channels, com.pangate.chat.core.StateCommands.NONE);
  }

  /** {@code state}: the CH3 ops ({@code chat.delivered}, {@code chat.read}, {@code chat.typing}, {@code chat.presence.watch}). */
  public UpstreamCommandListener(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic,
      Ingest ingest, Notifier notifier, CommandProducer dlq, Consumer<String> onOutcome,
      ChannelSubscriptions channels, com.pangate.chat.core.StateCommands state) {
    this(consumer, topic, ingest, notifier, dlq, onOutcome, channels, state, Role.INGEST);
  }

  public UpstreamCommandListener(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic,
      Ingest ingest, Notifier notifier, CommandProducer dlq, Consumer<String> onOutcome,
      ChannelSubscriptions channels, com.pangate.chat.core.StateCommands state, Role role) {
    this(consumer, topic, ingest, notifier, dlq, onOutcome, channels, state, role,
        KeyedPipeline.Settings.defaults(1, true), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
  }

  public UpstreamCommandListener(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic,
      Ingest ingest, Notifier notifier, CommandProducer dlq, Consumer<String> onOutcome,
      ChannelSubscriptions channels, com.pangate.chat.core.StateCommands state, Role role,
      KeyedPipeline.Settings settings, io.micrometer.core.instrument.MeterRegistry meters) {
    this.state = state;
    this.ingest = ingest;
    this.notifier = notifier;
    this.dlq = dlq;
    this.onOutcome = onOutcome;
    this.channels = channels;
    this.role = role;
    this.pipeline = new KeyedPipeline(role == Role.INGEST ? "ingest" : "state", consumer, topic, settings,
        this::handleAll, () -> { }, meters);
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap) {
    return consumer(bootstrap, GROUP, "earliest");
  }

  /** {@code autoOffsetReset}: {@code latest} for {@code chat-state} (spec §4.4: old state ops are not replayed). */
  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group, String autoOffsetReset) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    cfg.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return new KafkaConsumer<>(cfg, new StringDeserializer(), new ByteArrayDeserializer());
  }

  public void start() {
    pipeline.start();
  }

  /** One record a batch (the pipeline's {@code max-batch} 1); on the last try a deterministic failure is dead-lettered. */
  private void handleAll(List<ConsumerRecord<String, byte[]>> recs, boolean lastTry) {
    for (ConsumerRecord<String, byte[]> r : recs) {
      try {
        onOutcome.accept(handle(r));
      } catch (RuntimeException e) {
        if (lastTry && !PoisonGuard.isTransient(e)) {
          log.error("command at {}@{} failed {} times: {}; dead-lettered", r.topic(), r.offset(),
              PoisonGuard.MAX_ATTEMPTS, e.toString());
          dlq.deadLetter(r.key(), r.value(), "poison: " + e);
          onOutcome.accept("poison");
          continue;
        }
        onOutcome.accept("error");
        log.warn("command at {}@{} not handled: {}", r.topic(), r.offset(), e.toString());
        throw e;
      }
    }
  }

  private String handle(ConsumerRecord<String, byte[]> r) {
    UpstreamRecord u;
    try {
      u = UpstreamRecord.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException e) {
      if (role == Role.STATE || UserIds.fromPrincipal(r.key()).isEmpty()) {
        // the key is the sender's principal: another tenant's record is not ours to dead-letter; chat-ingest DLQs ours
        return "ignored";
      }
      dlq.deadLetter(r.key(), r.value(), "malformed UpstreamRecord");
      return "malformed";
    }
    String op = u.getHeadersOrDefault("op", "");
    if (!ChatTopics.TENANT.equals(u.getTenantId())) {
      return "ignored";
    }
    boolean stateOp = op.equals(ChatTopics.OP_DELIVERED) || op.equals(ChatTopics.OP_READ)
        || op.equals(ChatTopics.OP_TYPING) || op.equals(ChatTopics.OP_WATCH);
    if (stateOp != (role == Role.STATE)) {
      return "ignored";   // perf 3c (spec §4.4): the other role's op, or one neither takes
    }
    if (stateOp) {
      return state(r, u, op);
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

  /** CH3 spec §2: conversation-state ops. A payload that does not parse is dead-lettered like a bad chat.send. */
  private String state(ConsumerRecord<String, byte[]> r, UpstreamRecord u, String op) {
    Optional<String> user = UserIds.fromPrincipal(u.getPrincipalId());
    if (user.isEmpty()) {
      return "rejected";
    }
    try {
      return switch (op) {
        case ChatTopics.OP_DELIVERED, ChatTopics.OP_READ -> state.mark(user.get(), u.getDeviceId(),
            com.pangate.contracts.v1.ChatMark.parseFrom(u.getPayload()), op.equals(ChatTopics.OP_READ));
        case ChatTopics.OP_TYPING -> state.typing(user.get(), u.getDeviceId(),
            com.pangate.contracts.v1.ChatTyping.parseFrom(u.getPayload()));
        default -> state.watch(user.get(), u.getDeviceId(),
            com.pangate.contracts.v1.PresenceWatch.parseFrom(u.getPayload()));
      };
    } catch (InvalidProtocolBufferException e) {
      dlq.deadLetter(r.key(), r.value(), "malformed " + op);
      return "malformed";
    }
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
  public void close() {
    pipeline.close();
  }
}
