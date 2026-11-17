package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.TypingUpdate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** CH3 spec §5: typing goes straight to the other members as ephemeral pushes, limited. */
class TypingRelayTest {

  final FakeStores stores = new FakeStores();
  final List<PushRequest> pushed = new CopyOnWriteArrayList<>();
  final AtomicLong ms = new AtomicLong(1_000_000);
  Set<String> open = Set.of();
  final TypingRelay relay = new TypingRelay(stores, new MemberLookup(stores, stores), r -> {
    pushed.add(r);
    return CompletableFuture.completedFuture(PushResponse.getDefaultInstance());
  }, conv -> open, ms::get, 50);

  String conv(ConversationKind kind, int n) {
    String id = Ids.next();
    List<Member> ms = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      ms.add(new Member("u" + i, Role.MEMBER));
    }
    stores.create(new Conversation(id, kind, "t", "u0", 0, false, n), ms);
    return id;
  }

  @Test
  void dmTypingReachesPeerOnly() throws Exception {
    String d = conv(ConversationKind.CONVERSATION_KIND_DIRECT, 2);
    assertThat(relay.typing("u0", d)).isEqualTo("relayed");
    assertThat(pushed).singleElement().satisfies(r -> {
      assertThat(r.getPrincipalId()).isEqualTo("chat:u1");
      assertThat(r.getHeadersMap()).containsEntry("type", ChatTopics.TYPE_TYPING);
    });
    TypingUpdate u = TypingUpdate.parseFrom(pushed.getFirst().getPayload());
    assertThat(u.getUserId()).isEqualTo("u0");
    assertThat(u.getConversationId()).isEqualTo(d);
  }

  @Test
  void ttlIs6000AndClassEphemeral() throws Exception {
    String d = conv(ConversationKind.CONVERSATION_KIND_DIRECT, 2);
    relay.typing("u0", d);
    PushRequest r = pushed.getFirst();
    assertThat(r.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_EPHEMERAL);
    assertThat(r.getTtlSec()).isEqualTo(10);
    assertThat(TypingUpdate.parseFrom(r.getPayload()).getTtlMs()).isEqualTo(6000);
  }

  @Test
  void secondTypingWithin2sIsDropped() {
    String d = conv(ConversationKind.CONVERSATION_KIND_DIRECT, 2);
    relay.typing("u0", d);
    ms.addAndGet(1_999);
    assertThat(relay.typing("u0", d)).isEqualTo("too_often");
    ms.addAndGet(1);
    assertThat(relay.typing("u0", d)).isEqualTo("relayed");
    assertThat(pushed).hasSize(2);
  }

  @Test
  void channelTypingIsDropped() {
    String c = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, 3);
    assertThat(relay.typing("u0", c)).isEqualTo("channel");
    assertThat(pushed).isEmpty();
  }

  @Test
  void nonMemberIsDropped() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, 3);
    assertThat(relay.typing("stranger", g)).isEqualTo("not_member");
    assertThat(relay.typing("u0", Ids.next())).isEqualTo("not_member");
    assertThat(pushed).isEmpty();
  }

  @Test
  void largeGroupGoesOnlyToDevicesWithItOpen() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, 60);
    open = Set.of("u3/dA", "u7/dB", "u0/dC");   // the typer's own open device gets nothing
    relay.typing("u0", g);
    assertThat(pushed).extracting(r -> r.getPrincipalId() + "/" + r.getDeviceId())
        .containsExactlyInAnyOrder("chat:u3/dA", "chat:u7/dB");
  }

  @Test
  void smallGroupGoesToEveryOtherMember() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, 5);
    relay.typing("u0", g);
    assertThat(pushed).extracting(PushRequest::getPrincipalId)
        .containsExactlyInAnyOrder("chat:u1", "chat:u2", "chat:u3", "chat:u4");
  }

  @Test
  void perConversationFanOutAtMostOncePerSecond() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, 5);
    assertThat(relay.typing("u0", g)).isEqualTo("relayed");
    ms.addAndGet(500);
    assertThat(relay.typing("u1", g)).isEqualTo("conversation_busy");
    ms.addAndGet(500);
    assertThat(relay.typing("u1", g)).isEqualTo("relayed");
  }
}
