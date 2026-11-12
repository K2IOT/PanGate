package com.pangate.chat.core;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ConversationKind;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Checks a send in the order of chat spec §4.3 and writes it to {@code chat.commands}. Store errors
 * ({@link StoreUnavailableException}) and sink errors propagate: the caller answers 503 or retries.
 */
public final class Ingest {

  private final ChatStores.ConversationStore conversations;
  private final MemberLookup members;
  private final CommandSink sink;
  private final ChatProperties.Limits limits;
  private final LongSupplier clockMs;
  private final TokenBucketLimiter perUser;
  private final TokenBucketLimiter perConv;
  private final Consumer<String> onRejected;

  public Ingest(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      ChatStores.MemberCache cache, CommandSink sink, ChatProperties.Limits limits, LongSupplier nanos,
      LongSupplier clockMs) {
    this(conversations, members, cache, sink, limits, nanos, clockMs, reason -> { });
  }

  /** {@code onRejected(reason)} feeds the metric {@code chat.send.rejected{reason}}. */
  public Ingest(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      ChatStores.MemberCache cache, CommandSink sink, ChatProperties.Limits limits, LongSupplier nanos,
      LongSupplier clockMs, Consumer<String> onRejected) {
    this.conversations = conversations;
    this.members = new MemberLookup(members, cache);
    this.sink = sink;
    this.limits = limits;
    this.clockMs = clockMs;
    this.perUser = new TokenBucketLimiter(limits.userRate(), limits.userBurst(), nanos);
    this.perConv = new TokenBucketLimiter(limits.convRate(), limits.convBurst(), nanos);
    this.onRejected = onRejected;
  }

  public Optional<Rejection> accept(SendInput in) {
    Optional<Rejection> r = check(in);
    if (r.isPresent()) {
      onRejected.accept(r.get().reason());
      return r;
    }
    sink.publish(ChatCommand.newBuilder().setCommandId(Ids.next()).setSenderId(in.senderId())
        .setSenderDeviceId(in.senderDeviceId()).setReceivedAtMs(clockMs.getAsLong()).setSend(in.send())
        .setReplyTo(in.replyTo() == null ? "" : in.replyTo()).build());
    return Optional.empty();
  }

  private Optional<Rejection> check(SendInput in) {
    if (in.senderId() == null || in.senderId().isEmpty()) {
      return Optional.of(new Rejection(Rejection.INVALID, "sender is not a chat user"));
    }
    Optional<Rejection> invalid = SendValidator.check(in.send(), limits);
    if (invalid.isPresent()) {
      return invalid;
    }
    String convId = in.send().getConversationId();
    Optional<Conversation> conv = conversations.get(convId);
    if (conv.isEmpty()) {
      return Optional.of(new Rejection(Rejection.NOT_FOUND, "no such conversation"));
    }
    Optional<Member> me = members.of(convId).stream().filter(m -> m.userId().equals(in.senderId())).findFirst();
    if (me.isEmpty()) {
      return Optional.of(new Rejection(Rejection.NOT_MEMBER, "not a member"));
    }
    if (conv.get().kind() == ConversationKind.CONVERSATION_KIND_CHANNEL && me.get().role() == Role.MEMBER) {
      return Optional.of(new Rejection(Rejection.NOT_MEMBER, "only owners and admins post in a channel"));
    }
    if (!perUser.tryAcquire(in.senderId())) {
      return Optional.of(new Rejection(Rejection.RATE_LIMITED, "too many messages from this user"));
    }
    if (!perConv.tryAcquire(convId)) {
      return Optional.of(new Rejection(Rejection.RATE_LIMITED, "too many messages in this conversation"));
    }
    return Optional.empty();
  }
}
