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

  /** Final review I9: in a DM or small group two people typing are both relayed. */
  @Test
  void twoTypersInASmallConversationAreBothRelayed() {
    String d = conv(ConversationKind.CONVERSATION_KIND_DIRECT, 2);
    assertThat(relay.typing("u0", d)).isEqualTo("relayed");
    ms.addAndGet(300);
    assertThat(relay.typing("u1", d)).isEqualTo("relayed");
  }

  /** Spec §5 "Nhóm lớn": the per-conversation cap is for groups above listedMax. */
  @Test
  void perConversationFanOutAtMostOncePerSecond() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, 60);
    assertThat(relay.typing("u0", g)).isEqualTo("relayed");
    ms.addAndGet(500);
    assertThat(relay.typing("u1", g)).isEqualTo("conversation_busy");
    ms.addAndGet(500);
    assertThat(relay.typing("u1", g)).isEqualTo("relayed");
  }

  /** Perf 3a (spec §4): typing in a channel of 100 000 is refused before any member lookup. */
  @Test
  void channelTypingReadsNoMemberList() {
    String c = Ids.next();
    FakeStores big = FakeStores.withChannel(c, 100_000);
    TypingRelay r = new TypingRelay(big, new MemberLookup(big, big), x -> null, conv -> open, ms::get, 50,
        new TokenBucketLimiter(5, 10, () -> 0));
    assertThat(r.typing("f1", c)).isEqualTo("channel");
    assertThat(big.membersCalls.get()).isZero();
    assertThat(big.cachedMembersCalls.get()).isZero();
    assertThat(big.memberCalls.get()).isZero();
  }

  /** Perf 3a (spec §3.2): a per-user limit before any lookup. */
  @Test
  void strangerIsLimitedBeforeAnyLookup() {
    TypingRelay r = new TypingRelay(stores, new MemberLookup(stores, stores), x -> null, conv -> open, ms::get, 50,
        new TokenBucketLimiter(5, 10, () -> 0));
    for (int i = 0; i < 10; i++) {
      assertThat(r.typing("x", Ids.next())).isEqualTo("not_member");
    }
    int gets = stores.getCalls.get();
    assertThat(r.typing("x", Ids.next())).isEqualTo("rate_limited");
    assertThat(stores.getCalls.get()).isEqualTo(gets);
  }

  @Test
  void nonMemberOfGroupIsRefusedWithoutTheList() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, 3);
    stores.membersCalls.set(0);
    stores.cachedMembersCalls.set(0);
    assertThat(relay.typing("x", g)).isEqualTo("not_member");
    assertThat(stores.membersCalls.get()).isZero();
    assertThat(stores.cachedMembersCalls.get()).isZero();
  }
}
