package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.Topics;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * {@code chat.channel.open} / {@code chat.channel.close} (chat spec §6): a server-side CONTROL subscribe / unsubscribe
 * of {@code chat:chan:<convId>} for every connection of {@code chat:<userId>}. The gateway only checks the topic's
 * tenant and the per-connection limit, so the permission check lives here: a public channel, or a member of a private
 * one. Closing needs no permission. Sends are best effort; the client re-opens after a reconnect anyway.
 */
public final class ChannelSubscriptions {

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

  private static Topics topic(String convId) {
    return Topics.newBuilder().addTopics(ChatTopics.channelTopic(convId)).build();
  }

  private void send(String userId, ControlOp op) {
    pusher.send(PushRequest.newBuilder().setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_CONTROL)
        .setTenantId(ChatTopics.TENANT).setPrincipalId(UserIds.principal(userId)).setControl(op).build())
        .whenComplete((res, e) -> onResult.accept(e != null ? "unavailable"
            : res.getCode() == PushResponse.Code.CODE_OK ? "ok" : res.getCode().name()));
  }
}
