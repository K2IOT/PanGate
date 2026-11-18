package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.NotifyRequest;
import org.junit.jupiter.api.Test;

/** CH4 spec §3.5: what a notification says. */
class NotificationComposerTest {

  static final long SENT = 1_800_000_000_000L;

  static ChatMessage msg(String text) {
    return ChatMessage.newBuilder().setConversationId("C1").setMsgId("M1").setConvSeq(7).setSenderId("u1")
        .setText(text).setSentAtMs(SENT).build();
  }

  static NotifyRequest compose(ConversationKind kind, String text, boolean hide) {
    return NotificationComposer.compose(msg(text), kind, "Nhóm Dự án", "Lan", hide, 4, "u2", "chat");
  }

  @Test
  void dmTitleIsTheSender() {
    NotifyRequest r = compose(ConversationKind.CONVERSATION_KIND_DIRECT, "mai họp lúc 9h", false);
    assertThat(r.getAlert().getTitle()).isEqualTo("Lan");
    assertThat(r.getAlert().getBody()).isEqualTo("mai họp lúc 9h");
    assertThat(r.getPrincipalId()).isEqualTo("chat:u2");
    assertThat(r.getTenantId()).isEqualTo("chat");
    assertThat(r.getBadge()).isEqualTo(4);
  }

  @Test
  void groupTitleIsTheGroupAndBodyNamesTheSender() {
    NotifyRequest r = compose(ConversationKind.CONVERSATION_KIND_GROUP, "hi", false);
    assertThat(r.getAlert().getTitle()).isEqualTo("Nhóm Dự án");
    assertThat(r.getAlert().getBody()).isEqualTo("Lan: hi");
    assertThat(r.getCollapseId()).isEqualTo("M1");
  }

  @Test
  void channelCollapsesOnTheChannel() {
    NotifyRequest r = compose(ConversationKind.CONVERSATION_KIND_CHANNEL, "post", false);
    assertThat(r.getAlert().getTitle()).isEqualTo("Nhóm Dự án");
    assertThat(r.getAlert().getBody()).isEqualTo("post");
    assertThat(r.getCollapseId()).isEqualTo("C1");
  }

  @Test
  void longTextIsCutAt200CodePointsWithoutSplittingAnEmoji() {
    String text = "a".repeat(199) + "😀😀";
    String body = compose(ConversationKind.CONVERSATION_KIND_DIRECT, text, false).getAlert().getBody();
    assertThat(body.codePointCount(0, body.length())).isEqualTo(201);
    assertThat(body).isEqualTo("a".repeat(199) + "😀…");
    assertThat(compose(ConversationKind.CONVERSATION_KIND_DIRECT, "a".repeat(200), false).getAlert().getBody())
        .isEqualTo("a".repeat(200));
  }

  @Test
  void hiddenPreviewSaysNothing() {
    NotifyRequest r = compose(ConversationKind.CONVERSATION_KIND_GROUP, "secret", true);
    assertThat(r.getAlert().getTitle()).isEmpty();
    assertThat(r.getAlert().getBody()).isEmpty();
    assertThat(r.getAlert().getLocKey()).isEqualTo("notif_new_message");
  }

  @Test
  void noTextIsANewMessage() {
    assertThat(compose(ConversationKind.CONVERSATION_KIND_DIRECT, "", false).getAlert().getLocKey())
        .isEqualTo("notif_new_message");
  }

  @Test
  void idsExpiryAndData() {
    NotifyRequest r = compose(ConversationKind.CONVERSATION_KIND_DIRECT, "x", false);
    assertThat(r.getRequestId()).isEqualTo("M1/u2");
    assertThat(r.getExpiresAtMs()).isEqualTo(SENT + 3_600_000);
    assertThat(r.getThreadId()).isEqualTo("C1");
    assertThat(r.getHighPriority()).isTrue();
    assertThat(r.getDataMap()).containsEntry("conversationId", "C1").containsEntry("convSeq", "7")
        .containsEntry("msgId", "M1");
  }
}
