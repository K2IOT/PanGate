package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Final review I10: marks are hints (spec §3.3); a failing one must not hold up chat.send behind it. */
class StateIngestTest {

  final FakeStores stores = new FakeStores();
  final List<String> rejected = new ArrayList<>();
  final String conv = Ids.next();

  StateIngestTest() {
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_DIRECT, "d", "a", 0, false, 2),
        List.of(new Member("a", Role.MEMBER), new Member("b", Role.MEMBER)));
    stores.lastSeq.put(conv, 3L);
  }

  StateIngest ingest(MarkSink sink) {
    return new StateIngest(new MarkValidator(new MemberLookup(stores, stores), new LastSeqCache(stores, () -> 0L),
        new TokenBucketLimiter(50, 100, () -> 0L), rejected::add), sink);
  }

  @Test
  void queueDownDropsTheMarkWithoutThrowing() {
    StateIngest s = ingest(c -> {
      throw new QueueUnavailableException("kafka down", null);
    });
    assertThat(s.mark("b", "d1", ChatMark.newBuilder().setConversationId(conv).setUpToSeq(2).build(), true))
        .isEqualTo("rejected");
    assertThat(rejected).containsExactly("unavailable");
  }

  @Test
  void storeDownDropsTheMarkWithoutThrowing() {
    stores.fail = true;
    StateIngest s = ingest(c -> { });
    assertThat(s.mark("b", "d1", ChatMark.newBuilder().setConversationId(conv).setUpToSeq(2).build(), true))
        .isEqualTo("rejected");
    assertThat(rejected).containsExactly("unavailable");
  }
}
