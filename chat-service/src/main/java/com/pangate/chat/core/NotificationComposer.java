package com.pangate.chat.core;

import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.NotifyRequest;

/** CH4 spec §3.5: what a chat notification says, as a {@link NotifyRequest} for notify-service. */
public final class NotificationComposer {

  public static final String NEW_MESSAGE = "notif_new_message";
  static final int MAX_CODE_POINTS = 200;
  static final long TTL_MS = 3_600_000;

  private NotificationComposer() {}

  public static NotifyRequest compose(ChatMessage m, ConversationKind kind, String convTitle, String senderName,
      boolean hidePreview, int badge, String recipientUserId, String tenantId) {
    Alert.Builder a = Alert.newBuilder();
    if (hidePreview || m.getText().isEmpty()) {
      a.setLocKey(NEW_MESSAGE);
    } else {
      String text = cut(m.getText());
      switch (kind) {
        case CONVERSATION_KIND_DIRECT -> a.setTitle(senderName).setBody(text);
        case CONVERSATION_KIND_CHANNEL -> a.setTitle(convTitle).setBody(text);
        default -> a.setTitle(convTitle).setBody(senderName + ": " + text);
      }
    }
    boolean channel = kind == ConversationKind.CONVERSATION_KIND_CHANNEL;
    return NotifyRequest.newBuilder()
        .setRequestId(m.getMsgId() + "/" + recipientUserId)
        .setTenantId(tenantId)
        .setPrincipalId(ChatTopics.PRINCIPAL_PREFIX + recipientUserId)
        .setAlert(a)
        .setBadge(badge)
        .setThreadId(m.getConversationId())
        .setCollapseId(channel ? m.getConversationId() : m.getMsgId())
        .putData("conversationId", m.getConversationId())
        .putData("convSeq", Long.toString(m.getConvSeq()))
        .putData("msgId", m.getMsgId())
        .setCreatedAtMs(m.getSentAtMs())
        .setExpiresAtMs(m.getSentAtMs() + TTL_MS)
        .setHighPriority(true)
        .build();
  }

  /** At most 200 code points, then "…"; never splits a surrogate pair. */
  static String cut(String s) {
    if (s.codePointCount(0, s.length()) <= MAX_CODE_POINTS) {
      return s;
    }
    return s.substring(0, s.offsetByCodePoints(0, MAX_CODE_POINTS)) + "…";
  }
}
