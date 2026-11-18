package com.pangate.chat.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.contracts.v1.OfflineRecord;
import java.time.Duration;
import java.util.Optional;

/**
 * CH4 spec §3.1: a chat message no device of its recipient took (router's {@code OfflineRecord}) becomes a
 * notification, unless it is not chat's, too old, the recipient's own, or muted without a mention. Store failures
 * throw (the loop rewinds).
 */
public final class OfflineNotifications {

  /** {@code outcome}: sent, not_chat, invalid, own, stale, muted. */
  public record Result(String outcome, Optional<NotifyRequest> request) {
    static Result of(String outcome) {
      return new Result(outcome, Optional.empty());
    }
  }

  private final ChatStores.ConversationStore conversations;
  private final ChatStores.MuteStore mutes;
  private final PrivacyCache privacy;
  private final SenderNames names;
  private final Badge badge;
  private final long maxAgeMs;

  public OfflineNotifications(ChatStores.ConversationStore conversations, ChatStores.MuteStore mutes,
      PrivacyCache privacy, ChatStores.UserStore users, Badge badge, Duration maxAge) {
    this.conversations = conversations;
    this.mutes = mutes;
    this.privacy = privacy;
    this.names = new SenderNames(users);
    this.badge = badge;
    this.maxAgeMs = maxAge.toMillis();
  }

  public Result handle(OfflineRecord r, long nowMs) {
    var record = r.getRecord();
    if (!ChatTopics.TENANT.equals(record.getTenantId())
        || !FanOutPlanner.TYPE_MESSAGE.equals(record.getFrame().getHeadersOrDefault("type", ""))) {
      return Result.of("not_chat");
    }
    ChatMessage m;
    try {
      m = ChatMessage.parseFrom(record.getFrame().getPayload());
    } catch (InvalidProtocolBufferException e) {
      return Result.of("invalid");
    }
    String principal = record.getPrincipalId();
    if (!principal.startsWith(ChatTopics.PRINCIPAL_PREFIX)) {
      return Result.of("invalid");
    }
    String user = principal.substring(ChatTopics.PRINCIPAL_PREFIX.length());
    if (user.equals(m.getSenderId())) {
      return Result.of("own");
    }
    if (nowMs - m.getSentAtMs() > maxAgeMs) {
      return Result.of("stale");
    }
    Optional<ChatStores.Conversation> c = conversations.get(m.getConversationId());
    if (c.isEmpty()) {
      return Result.of("invalid");
    }
    if (!Mutes.shouldNotify(mutes.mutedUntil(m.getConversationId(), user), m.getMentionUserIdsList(), user, nowMs)) {
      return Result.of("muted");
    }
    boolean hide = privacy.of(user).hidePreview();
    NotifyRequest q = NotificationComposer.compose(m, c.get().kind(), c.get().title(), names.of(m.getSenderId(), nowMs),
        hide, badge.total(user), user, record.getTenantId());
    return new Result("sent", Optional.of(q));
  }

}
