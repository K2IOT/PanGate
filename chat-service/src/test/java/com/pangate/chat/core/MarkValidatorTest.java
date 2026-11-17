package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.testkit.ChatStateCases;
import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.ReceiptCommand;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** CH3 spec §3.3: marks are checked, clamped and limited before they reach chat.receipts. */
class MarkValidatorTest {

  final FakeStores stores = new FakeStores();
  final AtomicLong nanos = new AtomicLong();
  final List<String> rejected = new ArrayList<>();
  final MarkValidator v = new MarkValidator(new MemberLookup(stores, stores),
      new LastSeqCache(stores, () -> nanos.get() / 1_000_000), new TokenBucketLimiter(50, 100, nanos::get),
      rejected::add);
  final String conv = ChatStateCases.CONV;

  MarkValidatorTest() {
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 2),
        List.of(new Member("u1", Role.OWNER), new Member("u2", Role.MEMBER)));
    stores.lastSeq.put(conv, 10L);
  }

  Optional<ReceiptCommand> mark(String user, long seq, boolean read) {
    return v.check(user, "d1", ChatMark.newBuilder().setConversationId(conv).setUpToSeq(seq).build(), read, 1_000);
  }

  @Test
  void testkitCasesAreRefused() {
    for (ChatStateCases.MarkCase c : ChatStateCases.invalidMarks()) {
      rejected.clear();
      assertThat(v.check("u1", "d1", c.mark(), true, 1_000)).as(c.name()).isEmpty();
      assertThat(rejected).as(c.name()).containsExactly(c.reason());
    }
    for (ChatStateCases.MarkCase c : ChatStateCases.validMarks()) {
      assertThat(v.check("u1", "d1", c.mark(), true, 1_000)).as(c.name()).isPresent();
    }
  }

  @Test
  void nonMemberIsDropped() {
    assertThat(mark("stranger", 3, true)).isEmpty();
    assertThat(rejected).containsExactly("not_member");
  }

  /** Review Focus 1. */
  @Test
  void markAboveLastSeqIsClamped() {
    assertThat(mark("u2", 15, true)).get().satisfies(c -> {
      assertThat(c.getReadSeq()).isEqualTo(10);
      assertThat(c.getDeliveredSeq()).isEqualTo(10);
    });
  }

  @Test
  void readImpliesDelivered() {
    assertThat(mark("u2", 7, true)).get().satisfies(c -> {
      assertThat(c.getReadSeq()).isEqualTo(7);
      assertThat(c.getDeliveredSeq()).isEqualTo(7);
      assertThat(c.getUserId()).isEqualTo("u2");
      assertThat(c.getDeviceId()).isEqualTo("d1");
      assertThat(c.getConversationId()).isEqualTo(conv);
      assertThat(c.getReceivedAtMs()).isEqualTo(1_000);
    });
    assertThat(mark("u2", 4, false)).get().satisfies(c -> {
      assertThat(c.getDeliveredSeq()).isEqualTo(4);
      assertThat(c.getReadSeq()).isZero();
    });
  }

  @Test
  void rateLimitedAfterBurst() {
    for (int i = 0; i < 100; i++) {
      assertThat(mark("u2", 5, false)).isPresent();
    }
    assertThat(mark("u2", 5, false)).isEmpty();
    assertThat(rejected).containsExactly("rate_limited");
  }

  @Test
  void publicChannelFollowerMayMark() {
    String ch = com.pangate.chat.core.Ids.next();
    stores.create(new Conversation(ch, ConversationKind.CONVERSATION_KIND_CHANNEL, "c", "u1", 0, true, 2),
        List.of(new Member("u1", Role.OWNER), new Member("f1", Role.MEMBER)));
    stores.lastSeq.put(ch, 3L);
    assertThat(v.check("f1", "d", ChatMark.newBuilder().setConversationId(ch).setUpToSeq(2).build(), true, 1))
        .isPresent();
  }

  @Test
  void lastSeqIsCachedFor5Seconds() {
    assertThat(mark("u2", 12, true)).get().extracting(ReceiptCommand::getReadSeq).isEqualTo(10L);
    stores.lastSeq.put(conv, 12L);
    assertThat(mark("u2", 12, true)).get().extracting(ReceiptCommand::getReadSeq).isEqualTo(10L);
    nanos.addAndGet(5_001_000_000L);
    assertThat(mark("u2", 12, true)).get().extracting(ReceiptCommand::getReadSeq).isEqualTo(12L);
  }
}
