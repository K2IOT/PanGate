package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class ChannelSubscriptionsTest {

  final FakeStores stores = new FakeStores();
  final List<PushRequest> sent = new CopyOnWriteArrayList<>();
  final ChannelSubscriptions subs = new ChannelSubscriptions(stores, stores, r -> {
    sent.add(r);
    return CompletableFuture.completedFuture(PushResponse.newBuilder().setRequestId(r.getRequestId())
        .setCode(PushResponse.Code.CODE_OK).setOk(true).build());
  });

  String conv(ConversationKind kind, boolean isPublic) {
    String id = Ids.next();
    stores.create(new Conversation(id, kind, "c", "own", 0, isPublic, 1), List.of(new Member("own", Role.OWNER)));
    return id;
  }

  @Test
  void publicChannelSubscribes() {
    String c = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, true);
    assertThat(subs.open("fan", c)).isEmpty();
    assertThat(sent).singleElement().satisfies(r -> {
      assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_CONTROL);
      assertThat(r.getTenantId()).isEqualTo("chat");
      assertThat(r.getPrincipalId()).isEqualTo("chat:fan");
      assertThat(r.getControl().getSubscribe().getTopicsList()).containsExactly("chat:chan:" + c);
    });
  }

  @Test
  void privateChannelNeedsMembership() {
    String c = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, false);
    assertThat(subs.open("fan", c)).get().extracting(Rejection::reason).isEqualTo("not_member");
    assertThat(sent).isEmpty();
    stores.add(c, new Member("fan", Role.MEMBER));
    assertThat(subs.open("fan", c)).isEmpty();
    assertThat(sent).hasSize(1);
  }

  @Test
  void groupIsNotAChannel() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, false);
    assertThat(subs.open("own", g)).get().extracting(Rejection::reason).isEqualTo("invalid");
    assertThat(subs.open("own", Ids.next())).get().extracting(Rejection::reason).isEqualTo("not_found");
    assertThat(subs.open("own", "nope")).get().extracting(Rejection::reason).isEqualTo("invalid");
    assertThat(sent).isEmpty();
  }

  @Test
  void closeUnsubscribes() {
    String c = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, false);
    assertThat(subs.close("fan", c)).isEmpty();   // closing needs no permission
    assertThat(sent).singleElement().satisfies(r -> {
      assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_CONTROL);
      assertThat(r.getPrincipalId()).isEqualTo("chat:fan");
      assertThat(r.getControl().getUnsubscribe().getTopicsList()).containsExactly("chat:chan:" + c);
    });
    assertThat(subs.close("fan", "nope")).get().extracting(Rejection::reason).isEqualTo("invalid");
  }
}
