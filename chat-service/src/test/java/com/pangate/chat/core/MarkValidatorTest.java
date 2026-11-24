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

  /** Final review C1: a mark for a message newer than the cached last_seq is checked against a fresh read. */
  @Test
  void aNewerMessageWithinTheCacheWindowIsNotClamped() {
    assertThat(mark("u2", 10, true)).get().extracting(ReceiptCommand::getReadSeq).isEqualTo(10L);
    stores.lastSeq.put(conv, 11L);   // a sends message 11 one second later
    assertThat(mark("u2", 11, true)).get().extracting(ReceiptCommand::getReadSeq).isEqualTo(11L);
    assertThat(mark("u2", 15, true)).get().extracting(ReceiptCommand::getReadSeq).isEqualTo(11L);
  }

  /** Perf 3a (spec §4): a mark in a channel of 100 000 reads one member row. */
  @Test
  void channelMarkReadsNoMemberList() {
    String c = Ids.next();
    FakeStores big = FakeStores.withChannel(c, 100_000);
    big.lastSeq.put(c, 3L);
    MarkValidator mv = new MarkValidator(new MemberLookup(big, big), new LastSeqCache(big, () -> 0),
        new TokenBucketLimiter(50, 100, nanos::get), rejected::add);
    assertThat(mv.validate("f5", "d1", ChatMark.newBuilder().setConversationId(c).setUpToSeq(2).build(), true, 1)
        .command()).isPresent();
    assertThat(big.membersCalls.get()).isZero();
    assertThat(big.cachedMembersCalls.get()).isZero();
    assertThat(big.memberCalls.get()).isEqualTo(1);
  }

  /** Perf 3a (spec §3.2): the user limit comes before the member lookup. */
  @Test
  void strangerIsLimitedBeforeAnyLookup() {
    for (int i = 0; i < 100; i++) {
      v.validate("x", "d1", ChatMark.newBuilder().setConversationId(Ids.next()).setUpToSeq(1).build(), false, 1);
    }
    int before = stores.memberCalls.get();
    assertThat(v.validate("x", "d1", ChatMark.newBuilder().setConversationId(Ids.next()).setUpToSeq(1).build(), false,
        1).reason()).isEqualTo(MarkValidator.RATE_LIMITED);
    assertThat(stores.memberCalls.get()).isEqualTo(before);
  }

  /** Perf 3a (spec §1 anti-goal): no cached list keeps a removed member's right to mark. */
  @Test
  void removedMemberIsRefusedAtOnce() {
    assertThat(mark("u2", 5, true)).isPresent();
    stores.cache.put(conv, List.of(new Member("u1", Role.OWNER), new Member("u2", Role.MEMBER)));
    stores.members.get(conv).remove("u2");
    assertThat(v.validate("u2", "d1", ChatMark.newBuilder().setConversationId(conv).setUpToSeq(5).build(), true, 1)
        .reason()).isEqualTo(MarkValidator.NOT_MEMBER);
  }
}
