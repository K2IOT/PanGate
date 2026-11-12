package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.Topics;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code chat.channel.open} / {@code chat.channel.close} (chat spec §6): a server-side CONTROL subscribe / unsubscribe
 * of {@code chat:chan:<convId>} for every connection of {@code chat:<userId>}. The gateway only checks the topic's
 * tenant and the per-connection limit, so the permission check lives here: a public channel, or a member of a private
 * one. Closing needs no permission. A client's open / close is best effort (the client re-opens after a reconnect
 * anyway); a server-side {@link #revoke} after leaving a channel is retried.
 */
public final class ChannelSubscriptions {

  static final Duration REVOKE_FOR = Duration.ofMinutes(2);
  static final long INITIAL_BACKOFF_MS = 100;
  static final long MAX_BACKOFF_MS = 5_000;
  private static final Logger log = LoggerFactory.getLogger(ChannelSubscriptions.class);

  private final ChatStores.ConversationStore conversations;
  private final ChatStores.MemberStore members;
  private final Pusher pusher;
  private final Consumer<String> onResult;

  public ChannelSubscriptions(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      Pusher pusher) {
    this(conversations, members, pusher, result -> { });
  }

  /** {@code onResult}: ok, or the failing code / {@code unavailable} (metric). */
  public ChannelSubscriptions(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      Pusher pusher, Consumer<String> onResult) {
    this.conversations = conversations;
    this.members = members;
    this.pusher = pusher;
    this.onResult = onResult;
  }

  public Optional<Rejection> open(String userId, String convId) {
    if (!Ids.isUlid(convId)) {
      return Optional.of(new Rejection(Rejection.INVALID, "conversation_id must be a ULID"));
    }
    Optional<Conversation> c = conversations.get(convId);
    if (c.isEmpty()) {
      return Optional.of(new Rejection(Rejection.NOT_FOUND, "no such conversation"));
    }
    if (c.get().kind() != ConversationKind.CONVERSATION_KIND_CHANNEL) {
      return Optional.of(new Rejection(Rejection.INVALID, "not a channel"));
    }
    if (!c.get().isPublic() && members.member(convId, userId).isEmpty()) {
      return Optional.of(new Rejection(Rejection.NOT_MEMBER, "a private channel needs membership"));
    }
    send(userId, ControlOp.newBuilder().setSubscribe(topic(convId)).build());
    return Optional.empty();
  }

  public Optional<Rejection> close(String userId, String convId) {
    if (!Ids.isUlid(convId)) {
      return Optional.of(new Rejection(Rejection.INVALID, "conversation_id must be a ULID"));
    }
    send(userId, ControlOp.newBuilder().setUnsubscribe(topic(convId)).build());
    return Optional.empty();
  }

  /**
   * A member left or was removed from a channel: its open subscriptions must end even when Push API fails at first,
   * or a private channel's messages keep reaching it. The unsubscribe is retried with backoff (100 ms → 5 s) on
   * UNAVAILABLE, RATE_LIMITED or no answer, for at most {@link #REVOKE_FOR}; true once Push API accepted it.
   */
  public CompletableFuture<Boolean> revoke(String userId, String convId) {
    return revoke(userId, convId, INITIAL_BACKOFF_MS, System.nanoTime() + REVOKE_FOR.toNanos());
  }

  private CompletableFuture<Boolean> revoke(String userId, String convId, long backoffMs, long giveUpAt) {
    return send(userId, ControlOp.newBuilder().setUnsubscribe(topic(convId)).build()).handle((res, e) -> {
      if (e == null && res.getCode() == PushResponse.Code.CODE_OK) {
        return CompletableFuture.completedFuture(true);
      }
      boolean transientFailure = e != null || res.getCode() == PushResponse.Code.CODE_UNAVAILABLE
          || res.getCode() == PushResponse.Code.CODE_RATE_LIMITED;
      if (!transientFailure || System.nanoTime() > giveUpAt) {
        log.error("unsubscribe of {} from channel {} not accepted: {}", userId, convId,
            e != null ? e.toString() : res.getCode());
        onResult.accept("revoke_failed");
        return CompletableFuture.completedFuture(false);
      }
      return CompletableFuture.supplyAsync(() -> null,
              CompletableFuture.delayedExecutor(backoffMs, TimeUnit.MILLISECONDS))
          .thenCompose(x -> revoke(userId, convId, Math.min(MAX_BACKOFF_MS, backoffMs * 2), giveUpAt));
    }).thenCompose(f -> f);
  }

  private static Topics topic(String convId) {
    return Topics.newBuilder().addTopics(ChatTopics.channelTopic(convId)).build();
  }

  /** A fresh request id per attempt; never throws (a synchronous failure becomes a failed future). */
  private CompletableFuture<PushResponse> send(String userId, ControlOp op) {
    CompletableFuture<PushResponse> f;
    try {
      f = pusher.send(PushRequest.newBuilder().setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_CONTROL)
          .setTenantId(ChatTopics.TENANT).setPrincipalId(UserIds.principal(userId)).setControl(op).build());
    } catch (RuntimeException e) {
      f = CompletableFuture.failedFuture(e);
    }
    return f.whenComplete((res, e) -> onResult.accept(e != null ? "unavailable"
        : res.getCode() == PushResponse.Code.CODE_OK ? "ok" : res.getCode().name()));
  }
}
