package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Member;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * What one chat event becomes at the Push API (chat spec §6): DIRECT / GROUP → one RELIABLE MESSAGE per member, no
 * idempotency key; CHANNEL → one EPHEMERAL BROADCAST on {@code chat:chan:<convId>}. Request ids are ULIDs.
 */
public final class FanOutPlanner {

  public static final String TYPE_MESSAGE = "chat.message";
  public static final String TYPE_REJECTED = "chat.rejected";

  private FanOutPlanner() {}

  public static List<PushRequest> plan(ChatEvent e, List<Member> members) {
    String conv = e.getMessage().getConversationId();
    PushRequest.Builder base = PushRequest.newBuilder().setTenantId(ChatTopics.TENANT)
        .setPayload(e.getMessage().toByteString()).putHeaders("type", TYPE_MESSAGE)
        .setSenderPrincipalId(UserIds.principal(e.getMessage().getSenderId()));
    if (e.getKind() == ConversationKind.CONVERSATION_KIND_CHANNEL) {
      return List.of(base.setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_BROADCAST)
          .setTopic(ChatTopics.channelTopic(conv)).setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL).build());
    }
    List<PushRequest> out = new ArrayList<>(members.size());
    for (Member m : members) {
      out.add(base.clone().setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_MESSAGE)
          .setPrincipalId(UserIds.principal(m.userId())).setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE).build());
    }
    return out;
  }

  /** A refused WebSocket send, to the sender's online devices (chat spec §4.1 step 3). */
  public static PushRequest rejected(String senderId, ChatRejected r) {
    return PushRequest.newBuilder().setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_MESSAGE)
        .setTenantId(ChatTopics.TENANT).setPrincipalId(UserIds.principal(senderId))
        .setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL).putHeaders("type", TYPE_REJECTED)
        .setPayload(r.toByteString()).build();
  }
}
