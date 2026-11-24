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
  private final UserDirectory users;

  public Ingest(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      ChatStores.MemberCache cache, CommandSink sink, ChatProperties.Limits limits, LongSupplier nanos,
      LongSupplier clockMs) {
    this(conversations, members, cache, sink, limits, nanos, clockMs, reason -> { });
  }

  /** {@code onRejected(reason)} feeds the metric {@code chat.send.rejected{reason}}. */
  public Ingest(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      ChatStores.MemberCache cache, CommandSink sink, ChatProperties.Limits limits, LongSupplier nanos,
      LongSupplier clockMs, Consumer<String> onRejected) {
    this(conversations, members, cache, sink, limits, nanos, clockMs, onRejected, UserDirectory.ANY);
  }

  /** {@code users}: a DM whose other side deleted the account is refused ({@code user_deleted}, account spec §8). */
  public Ingest(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      ChatStores.MemberCache cache, CommandSink sink, ChatProperties.Limits limits, LongSupplier nanos,
      LongSupplier clockMs, Consumer<String> onRejected, UserDirectory users) {
    this.users = users;
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
    return accept(in, false);
  }

  /**
   * Final review I3 (perf 3a §3.2): takes one of the user's send tokens, for a caller that must read a store before
   * {@link #acceptAdmitted} (the HTTP send's prior-send lookup). False is counted as {@code rate_limited}.
   */
  public boolean admit(String senderId) {
    if (perUser.tryAcquire(senderId)) {
      return true;
    }
    onRejected.accept(Rejection.RATE_LIMITED);
    return false;
  }

  /** {@link #accept} for a send whose user token {@link #admit} already took. */
  public Optional<Rejection> acceptAdmitted(SendInput in) {
    return accept(in, true);
  }

  private Optional<Rejection> accept(SendInput in, boolean admitted) {
    Optional<Rejection> r = check(in, admitted);
    if (r.isPresent()) {
      onRejected.accept(r.get().reason());
      return r;
    }
    sink.publish(ChatCommand.newBuilder().setCommandId(Ids.next()).setSenderId(in.senderId())
        .setSenderDeviceId(in.senderDeviceId()).setReceivedAtMs(clockMs.getAsLong()).setSend(in.send())
        .setReplyTo(in.replyTo() == null ? "" : in.replyTo()).build());
    return Optional.empty();
  }

  private Optional<Rejection> check(SendInput in, boolean admitted) {
    if (in.senderId() == null || in.senderId().isEmpty()) {
      return Optional.of(new Rejection(Rejection.INVALID, "sender is not a chat user"));
    }
    Optional<Rejection> invalid = SendValidator.check(in.send(), limits);
    if (invalid.isPresent()) {
      return invalid;
    }
    // perf 3a (spec §3.2): the user's limit before any lookup, so a stranger's sends cost no store reads
    if (!admitted && !perUser.tryAcquire(in.senderId())) {
      return Optional.of(new Rejection(Rejection.RATE_LIMITED, "too many messages from this user"));
    }
    String convId = in.send().getConversationId();
    Optional<Conversation> conv = conversations.get(convId);
    if (conv.isEmpty()) {
      return Optional.of(new Rejection(Rejection.NOT_FOUND, "no such conversation"));
    }
    Optional<Member> me = members.member(convId, in.senderId());
    if (me.isEmpty()) {
      return Optional.of(new Rejection(Rejection.NOT_MEMBER, "not a member"));
    }
    // a DM's list is two rows: the other side comes from it
    if (conv.get().kind() == ConversationKind.CONVERSATION_KIND_DIRECT && members.of(convId).stream()
        .anyMatch(m -> !m.userId().equals(in.senderId()) && users.isDeleted(m.userId()))) {
      return Optional.of(new Rejection(Rejection.USER_DELETED, "the other user deleted the account"));
    }
    if (conv.get().kind() == ConversationKind.CONVERSATION_KIND_CHANNEL && me.get().role() == Role.MEMBER) {
      return Optional.of(new Rejection(Rejection.NOT_MEMBER, "only owners and admins post in a channel"));
    }
    if (!perConv.tryAcquire(convId)) {
      return Optional.of(new Rejection(Rejection.RATE_LIMITED, "too many messages in this conversation"));
    }
    return Optional.empty();
  }
}
