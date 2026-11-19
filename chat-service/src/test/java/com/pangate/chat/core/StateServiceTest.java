package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ConversationKind;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** CH3 spec §3.2, §4: unread counts. */
class StateServiceTest {

  @Test
  void unreadIsClampedTo0And999() {
    assertThat(StateService.unread(5, 9)).isZero();
    assertThat(StateService.unread(2000, 0)).isEqualTo(999);
    assertThat(StateService.unread(12, 10)).isEqualTo(2);
  }

  @Test
  void channelUnreadUsesConversationLastSeq() {
    FakeStores stores = new FakeStores();
    String ch = Ids.next();
    stores.create(new Conversation(ch, ConversationKind.CONVERSATION_KIND_CHANNEL, "c", "o", 0, true, 2),
        List.of(new Member("o", Role.OWNER), new Member("f", Role.MEMBER)));
    stores.lastSeq.put(ch, 30L);
    stores.advance(ch, "f", 25, 25, 1);
    StateService state = new StateService(stores, new MemberLookup(stores, stores), stores, stores, stores,
        new PrivacyCache(stores, () -> 0L), 50, () -> 0L, System::currentTimeMillis);
    ChatStores.UserConv row = stores.userConvs("f", null, 10).getFirst();
    Map<String, Object> v = state.conversationView("f", new ChatStores.UserConvState(row.convId(),
        ConversationKind.CONVERSATION_KIND_CHANNEL, row.lastSeq(), row.readSeq(), row.deliveredSeq(), 0));
    assertThat(v).containsEntry("lastSeq", 30L).containsEntry("readSeq", 25L).containsEntry("unread", 5);
  }

  /** Final review I5: "now" leaves a margin, so a change written just before it (or by a slower pod) is not missed. */
  @Test
  void stateNowLeavesAMarginForInFlightWrites() {
    FakeStores stores = new FakeStores();
    StateService state = new StateService(stores, new MemberLookup(stores, stores), stores, stores, stores,
        new PrivacyCache(stores, () -> 0L), 50, () -> 0L, () -> 100_000L);
    assertThat(state.state("u", java.util.OptionalLong.empty(), null).now()).isEqualTo(100_000 - StateService.SINCE_MARGIN_MS);
    assertThat(StateService.SINCE_MARGIN_MS).isEqualTo(30_000);
  }

  /** Perf 3a (spec §3.4): a channel's lastSeq in the conversation list comes from the 5 s cache. */
  @Test
  void channelLastSeqComesFromCache() {
    FakeStores stores = new FakeStores();
    String ch = Ids.next();
    stores.create(new Conversation(ch, ConversationKind.CONVERSATION_KIND_CHANNEL, "c", "o", 0, true, 2),
        List.of(new Member("o", Role.OWNER), new Member("f", Role.MEMBER)));
    stores.lastSeq.put(ch, 30L);
    StateService state = new StateService(stores, new MemberLookup(stores, stores), stores, stores, stores,
        new PrivacyCache(stores, () -> 0L), 50, () -> 0L, System::currentTimeMillis);
    ChatStores.UserConvState s = new ChatStores.UserConvState(ch, ConversationKind.CONVERSATION_KIND_CHANNEL, 0, 0,
        0, 0);
    stores.lastSeqCalls.set(0);
    state.conversationView("f", s);
    state.conversationView("f", s);
    assertThat(stores.lastSeqCalls.get()).isEqualTo(1);
  }
}
