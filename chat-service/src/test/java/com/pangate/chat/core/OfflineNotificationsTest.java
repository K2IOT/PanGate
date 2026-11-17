package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** CH4 spec §3.1: which offline records become notifications. */
class OfflineNotificationsTest {

  static final long NOW = 1_800_000_000_000L;
  final FakeStores stores = new FakeStores();
  final String g = Ids.next();
  final OfflineNotifications n;

  OfflineNotificationsTest() {
    stores.create(new Conversation(g, ConversationKind.CONVERSATION_KIND_GROUP, "Nhóm", "u1", 0, false, 2),
        List.of(new Member("u1", Role.OWNER), new Member("u2", Role.MEMBER)));
    stores.put("u1", "active", "Lan", 1);
    n = new OfflineNotifications(stores, stores, new PrivacyCache(stores, () -> 0L), stores, new Badge(stores),
        Duration.ofHours(1));
  }

  OfflineRecord rec(String tenant, String type, String recipient, ChatMessage m) {
    return OfflineRecord.newBuilder().setRecord(PushRecord.newBuilder().setTenantId(tenant)
        .setPrincipalId("chat:" + recipient).setFrame(Frame.newBuilder().setPayload(m.toByteString())
            .putHeaders("type", type))).setReason(OfflineRecord.Reason.REASON_NO_SESSION).build();
  }

  ChatMessage msg(long sentAt, String... mentions) {
    return ChatMessage.newBuilder().setConversationId(g).setMsgId("M1").setConvSeq(1).setSenderId("u1")
        .setText("hi").setSentAtMs(sentAt).addAllMentionUserIds(List.of(mentions)).build();
  }

  @Test
  void aChatMessageToAnOfflineMemberIsSent() {
    OfflineNotifications.Result r = n.handle(rec("chat", "chat.message", "u2", msg(NOW - 1_000)), NOW);
    assertThat(r.outcome()).isEqualTo("sent");
    assertThat(r.request()).get().satisfies(q -> {
      assertThat(q.getAlert().getTitle()).isEqualTo("Nhóm");
      assertThat(q.getAlert().getBody()).isEqualTo("Lan: hi");
      assertThat(q.getPrincipalId()).isEqualTo("chat:u2");
    });
  }

  @Test
  void otherTenantsAndTypesAreNotChat() {
    assertThat(n.handle(rec("other", "chat.message", "u2", msg(NOW)), NOW).outcome()).isEqualTo("not_chat");
    assertThat(n.handle(rec("chat", "chat.rejected", "u2", msg(NOW)), NOW).outcome()).isEqualTo("not_chat");
  }

  @Test
  void olderThanAnHourIsStale() {
    assertThat(n.handle(rec("chat", "chat.message", "u2", msg(NOW - 3_600_001)), NOW).outcome())
        .isEqualTo("stale");
  }

  @Test
  void mutedUnlessMentioned() {
    stores.setMute(g, "u2", Long.MAX_VALUE);
    assertThat(n.handle(rec("chat", "chat.message", "u2", msg(NOW)), NOW).outcome()).isEqualTo("muted");
    assertThat(n.handle(rec("chat", "chat.message", "u2", msg(NOW, "u2")), NOW).outcome()).isEqualTo("sent");
  }

  @Test
  void theSendersOwnMessageIsNotNotified() {
    assertThat(n.handle(rec("chat", "chat.message", "u1", msg(NOW)), NOW).outcome()).isEqualTo("own");
  }

  @Test
  void unknownNameFallsBackToTheUserId() {
    ChatMessage m = msg(NOW).toBuilder().setSenderId("u3").build();
    stores.add(g, new Member("u3", Role.MEMBER));
    assertThat(n.handle(rec("chat", "chat.message", "u2", m), NOW).request()).get()
        .satisfies(q -> assertThat(q.getAlert().getBody()).isEqualTo("u3: hi"));
  }
}
