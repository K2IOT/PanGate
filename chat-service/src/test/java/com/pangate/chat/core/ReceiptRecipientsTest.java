package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Marks;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Privacy;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.ReceiptUpdate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** CH3 spec §3.4, §8: who gets which receipt update. */
class ReceiptRecipientsTest {

  static List<Member> members(int n) {
    List<Member> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(new Member("u" + i, Role.MEMBER));
    }
    return out;
  }

  static ReceiptAggregator.Flush flush(ConversationKind kind, Map<String, Marks> byUser) {
    return new ReceiptAggregator.Flush("c1", kind, byUser);
  }

  static final Function<String, Privacy> OPEN = u -> Privacy.OPEN;

  static ReceiptUpdate payload(PushRequest r) throws Exception {
    return ReceiptUpdate.parseFrom(r.getPayload());
  }

  static List<PushRequest> to(List<PushRequest> all, String user) {
    return all.stream().filter(r -> r.getPrincipalId().equals("chat:" + user)).toList();
  }

  @Test
  void dmPushesPeerAndOwnDevices() throws Exception {
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("u0", new Marks(4, 3))), members(2), OPEN, 50);
    assertThat(out).allSatisfy(r -> {
      assertThat(r.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_EPHEMERAL);
      assertThat(r.getHeadersMap()).containsEntry("type", ChatTopics.TYPE_RECEIPTS);
      assertThat(r.getTtlSec()).isEqualTo(30);
      assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_MESSAGE);
    });
    assertThat(payload(to(out, "u0").getFirst()).getOwnReadSeq()).isEqualTo(3);
    assertThat(to(out, "u0")).hasSize(1);
    ReceiptUpdate peer = payload(to(out, "u1").getFirst());
    assertThat(peer.getMarksList()).singleElement().satisfies(m -> {
      assertThat(m.getUserId()).isEqualTo("u0");
      assertThat(m.getDeliveredSeq()).isEqualTo(4);
      assertThat(m.getReadSeq()).isEqualTo(3);
    });
  }

  @Test
  void deliveredOnlySendsNoOwnSync() {
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("u0", new Marks(4, 0))), members(2), OPEN, 50);
    assertThat(to(out, "u0")).isEmpty();
    assertThat(to(out, "u1")).hasSize(1);
  }

  @Test
  void group50ListsMarks() throws Exception {
    Map<String, Marks> by = new LinkedHashMap<>();
    by.put("u1", new Marks(9, 9));
    by.put("u2", new Marks(9, 7));
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_GROUP, by), members(50),
        OPEN, 50);
    assertThat(payload(to(out, "u5").getFirst()).getMarksList()).hasSize(2);
    assertThat(payload(to(out, "u1").stream().filter(r -> {
      try {
        return payload(r).getMarksCount() > 0;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }).findFirst().orElseThrow()).getMarksList()).singleElement()
        .satisfies(m -> assertThat(m.getUserId()).isEqualTo("u2"));
    assertThat(out).hasSize(50 + 2);   // a marks update to each member, an own sync to each reader
  }

  @Test
  void group51PushesNobodyButOwnDevices() {
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_GROUP,
        Map.of("u1", new Marks(9, 9))), members(51), OPEN, 50);
    assertThat(out).singleElement().satisfies(r -> assertThat(r.getPrincipalId()).isEqualTo("chat:u1"));
  }

  @Test
  void channelPushesNobodyButOwnDevices() {
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_CHANNEL,
        Map.of("u1", new Marks(9, 9))), members(3), OPEN, 50);
    assertThat(out).singleElement().satisfies(r -> assertThat(r.getPrincipalId()).isEqualTo("chat:u1"));
  }

  static ReceiptAggregator.Flush flush(ConversationKind kind, Map<String, Marks> byUser,
      Map<String, Long> deliveredOnly) {
    return new ReceiptAggregator.Flush("c1", kind, byUser, deliveredOnly);
  }

  /** Final review C3: a hidden read alone must produce no push to others (its timing would say "seen"). */
  @Test
  void hiddenReadAloneSendsNothingToOthers() throws Exception {
    Function<String, Privacy> privacy = u -> u.equals("u0") ? new Privacy(false, true) : Privacy.OPEN;
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("u0", new Marks(4, 4))), members(2), privacy, 50);
    assertThat(to(out, "u1")).isEmpty();
    assertThat(payload(to(out, "u0").getFirst()).getOwnReadSeq()).isEqualTo(4);   // own sync stays (S4)
    Function<String, Privacy> viewerHides = u -> u.equals("u1") ? new Privacy(false, true) : Privacy.OPEN;
    assertThat(to(ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("u0", new Marks(4, 4))), members(2), viewerHides, 50), "u1")).isEmpty();
  }

  @Test
  void hideReadZeroesReadButKeepsDelivered() throws Exception {
    Function<String, Privacy> privacy = u -> u.equals("u0") ? new Privacy(false, true) : Privacy.OPEN;
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("u0", new Marks(4, 3)), Map.of("u0", 4L)), members(2), privacy, 50);
    assertThat(payload(to(out, "u1").getFirst()).getMarksList()).singleElement().satisfies(m -> {
      assertThat(m.getDeliveredSeq()).isEqualTo(4);
      assertThat(m.getReadSeq()).isZero();
    });
    // the reader's own devices still sync (S4)
    assertThat(payload(to(out, "u0").getFirst()).getOwnReadSeq()).isEqualTo(3);
    // reciprocal: a viewer who hides reads sees nobody's
    Function<String, Privacy> viewerHides = u -> u.equals("u1") ? new Privacy(false, true) : Privacy.OPEN;
    List<PushRequest> out2 = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("u0", new Marks(4, 3)), Map.of("u0", 4L)), members(2), viewerHides, 50);
    assertThat(payload(to(out2, "u1").getFirst()).getMarks(0).getReadSeq()).isZero();
  }

  /** Review Focus 2: a user no longer in the conversation neither receives nor is listed. */
  @Test
  void leftMemberGetsNothing() throws Exception {
    Map<String, Marks> by = new LinkedHashMap<>();
    by.put("gone", new Marks(5, 5));
    by.put("u1", new Marks(5, 5));
    List<PushRequest> out = ReceiptRecipients.plan(flush(ConversationKind.CONVERSATION_KIND_GROUP, by), members(3),
        OPEN, 50);
    assertThat(to(out, "gone")).isEmpty();
    assertThat(payload(to(out, "u2").getFirst()).getMarksList()).extracting(ReceiptUpdate.Mark::getUserId)
        .containsExactly("u1");
  }

  /** Review 2026-11-19 (F3): the other member of a DM sees its state change only when there is something to see. */
  @Test
  void aHiddenReadDoesNotTouchThePeersState() {
    Function<String, Privacy> bHides = u -> u.equals("b") ? new Privacy(false, true) : Privacy.OPEN;
    ReceiptAggregator.Flush readOnly = new ReceiptAggregator.Flush("c1", ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("b", new Marks(5, 5)), Map.of());
    assertThat(ReceiptRecipients.peerSeesChange(readOnly, "a", bHides)).isFalse();
    assertThat(ReceiptRecipients.peerSeesChange(readOnly, "a", OPEN)).isTrue();
    ReceiptAggregator.Flush delivered = new ReceiptAggregator.Flush("c1", ConversationKind.CONVERSATION_KIND_DIRECT,
        Map.of("b", new Marks(5, 5)), Map.of("b", 4L));
    assertThat(ReceiptRecipients.peerSeesChange(delivered, "a", bHides)).as("a real chat.delivered shows").isTrue();
  }

  /** Perf 3a (spec §3.1): a channel's flush never loads its follower list. */
  @Test
  void channelAudienceReadsNoMemberList() {
    FakeStores big = FakeStores.withChannel("c1", 100_000);
    Map<String, Marks> by = new java.util.LinkedHashMap<>();
    for (String u : List.of("f1", "f2", "f3")) {
      by.put(u, new Marks(4, 4));
    }
    ReceiptAggregator.Flush f = flush(ConversationKind.CONVERSATION_KIND_CHANNEL, by);
    List<Member> audience = ReceiptRecipients.audience(f, new MemberLookup(big, big));
    assertThat(audience).extracting(Member::userId).containsExactlyInAnyOrder("f1", "f2", "f3");
    assertThat(big.membersCalls.get()).isZero();
    assertThat(big.cachedMembersCalls.get()).isZero();
    assertThat(ReceiptRecipients.plan(f, audience, OPEN, 50)).hasSize(3)
        .allSatisfy(r -> assertThat(r.getHeadersMap()).containsEntry("type", ChatTopics.TYPE_RECEIPTS));
  }
}
