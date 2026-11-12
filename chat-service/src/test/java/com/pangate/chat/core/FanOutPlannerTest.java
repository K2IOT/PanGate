package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRequest;
import java.util.List;
import org.junit.jupiter.api.Test;

class FanOutPlannerTest {

  static ChatEvent event(ConversationKind kind) {
    return ChatEvent.newBuilder().setKind(kind).setMessage(ChatMessage.newBuilder().setConversationId("C1")
        .setMsgId("M1").setConvSeq(4).setSenderId("u1").setText("hi")).build();
  }

  @Test
  void groupGetsOneReliableMessagePerMember() throws Exception {
    List<PushRequest> rs = FanOutPlanner.plan(event(ConversationKind.CONVERSATION_KIND_GROUP), List.of(
        new Member("u1", Role.OWNER), new Member("u2", Role.MEMBER), new Member("u3", Role.MEMBER)));
    assertThat(rs).extracting(PushRequest::getPrincipalId).containsExactly("chat:u1", "chat:u2", "chat:u3");
    assertThat(rs).allSatisfy(r -> {
      assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_MESSAGE);
      assertThat(r.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_RELIABLE);
      assertThat(r.getTenantId()).isEqualTo("chat");
      assertThat(r.getIdempotencyKey()).isEmpty();
      assertThat(r.getHeadersMap()).containsEntry("type", "chat.message");
      assertThat(r.getSenderPrincipalId()).isEqualTo("chat:u1");
      assertThat(ChatMessage.parseFrom(r.getPayload())).isEqualTo(event(ConversationKind.CONVERSATION_KIND_GROUP)
          .getMessage());
    });
    assertThat(rs).extracting(PushRequest::getRequestId).doesNotHaveDuplicates();
    assertThat(FanOutPlanner.plan(event(ConversationKind.CONVERSATION_KIND_DIRECT), List.of(
        new Member("u1", Role.MEMBER), new Member("u2", Role.MEMBER)))).hasSize(2);
  }

  @Test
  void channelIsOneEphemeralBroadcast() throws Exception {
    List<PushRequest> rs = FanOutPlanner.plan(event(ConversationKind.CONVERSATION_KIND_CHANNEL), List.of());
    assertThat(rs).singleElement().satisfies(r -> {
      assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_BROADCAST);
      assertThat(r.getTopic()).isEqualTo("chat:chan:C1");
      assertThat(r.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_EPHEMERAL);
      assertThat(r.getTenantId()).isEqualTo("chat");
      assertThat(r.getPrincipalId()).isEmpty();
      assertThat(ChatMessage.parseFrom(r.getPayload()).getMsgId()).isEqualTo("M1");
    });
  }

  @Test
  void rejectionIsAnEphemeralMessageToTheSender() throws Exception {
    ChatRejected rej = ChatRejected.newBuilder().setConversationId("C1").setClientMsgId("K").setReason("not_member")
        .build();
    PushRequest r = FanOutPlanner.rejected("u9", rej);
    assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_MESSAGE);
    assertThat(r.getPrincipalId()).isEqualTo("chat:u9");
    assertThat(r.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_EPHEMERAL);
    assertThat(r.getHeadersMap()).containsEntry("type", "chat.rejected");
    assertThat(ChatRejected.parseFrom(r.getPayload())).isEqualTo(rej);
  }
}
