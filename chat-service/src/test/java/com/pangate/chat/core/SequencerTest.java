package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class SequencerTest {

  final FakeStores stores = new FakeStores();
  /** A second dedup store standing in for Redis. */
  final Map<String, StoredMessage> redis = new ConcurrentHashMap<>();
  final ChatStores.DedupStore cache = new ChatStores.DedupStore() {
    @Override
    public java.util.Optional<StoredMessage> find(String c, String s, String k) {
      return java.util.Optional.ofNullable(redis.get(c + "|" + s + "|" + k));
    }

    @Override
    public void put(StoredMessage m) {
      redis.put(m.convId() + "|" + m.senderId() + "|" + m.clientMsgId(), m);
    }
  };
  final Sequencer seq = new Sequencer(stores, stores, stores, cache, () -> 1_000L);

  String conv(long lastSeq) {
    String id = Ids.next();
    stores.create(new Conversation(id, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
    stores.lastSeq.put(id, lastSeq);
    return id;
  }

  static ChatCommand cmd(String conv, String clientMsgId, String text) {
    return ChatCommand.newBuilder().setCommandId(Ids.next()).setSenderId("u1").setSenderDeviceId("d1")
        .setReceivedAtMs(900).setSend(ChatSend.newBuilder().setConversationId(conv).setClientMsgId(clientMsgId)
            .setText(text)).build();
  }

  @Test
  void batchGetsConsecutiveSeqs() {
    String c = conv(7);
    List<SequencedMessage> out = seq.process(c, List.of(cmd(c, Ids.next(), "a"), cmd(c, Ids.next(), "b"),
        cmd(c, Ids.next(), "c")));
    assertThat(out).extracting(m -> m.message().convSeq()).containsExactly(8L, 9L, 10L);
    assertThat(out).extracting(m -> m.message().text()).containsExactly("a", "b", "c");
    assertThat(out).noneMatch(SequencedMessage::duplicate);
    assertThat(stores.reserveCalls).isEqualTo(1);
    assertThat(stores.lastSeq.get(c)).isEqualTo(10L);
    assertThat(stores.messages.get(c)).containsOnlyKeys(8L, 9L, 10L);
    assertThat(stores.messages.get(c).get(8L).sentAtMs()).isEqualTo(1_000L);
    assertThat(stores.dedup).hasSize(3);
    assertThat(redis).hasSize(3);
    // the next batch continues from memory: one more reserve, no re-read needed
    assertThat(seq.process(c, List.of(cmd(c, Ids.next(), "d")))).extracting(m -> m.message().convSeq())
        .containsExactly(11L);
  }

  @Test
  void duplicateReusesSeqAndIsReEmitted() {
    String c = conv(0);
    String k = Ids.next();
    SequencedMessage first = seq.process(c, List.of(cmd(c, k, "hi"))).getFirst();
    int reserves = stores.reserveCalls;
    redis.clear();   // Redis lost it: ScyllaDB still knows
    List<SequencedMessage> again = seq.process(c, List.of(cmd(c, k, "hi"), cmd(c, Ids.next(), "new")));
    assertThat(again.get(0).duplicate()).isTrue();
    assertThat(again.get(0).message().msgId()).isEqualTo(first.message().msgId());
    assertThat(again.get(0).message().convSeq()).isEqualTo(1L);
    assertThat(again.get(0).message().text()).isEqualTo("hi");
    assertThat(again.get(1).message().convSeq()).isEqualTo(2L);
    assertThat(stores.reserveCalls).isEqualTo(reserves + 1);
    // the same key twice in one batch is one message too
    String k2 = Ids.next();
    List<SequencedMessage> twice = seq.process(c, List.of(cmd(c, k2, "x"), cmd(c, k2, "x")));
    assertThat(twice.get(1).duplicate()).isTrue();
    assertThat(twice.get(1).message().convSeq()).isEqualTo(twice.get(0).message().convSeq());
    assertThat(stores.lastSeq.get(c)).isEqualTo(3L);
  }

  @Test
  void onlyDuplicatesReserveNothing() {
    String c = conv(0);
    String k = Ids.next();
    seq.process(c, List.of(cmd(c, k, "hi")));
    int reserves = stores.reserveCalls;
    assertThat(seq.process(c, List.of(cmd(c, k, "hi")))).singleElement().matches(SequencedMessage::duplicate);
    assertThat(stores.reserveCalls).isEqualTo(reserves);
  }

  /** Review Focus 5: the dedup key includes the conversation. */
  @Test
  void sameClientMsgIdInTwoConversationsIsTwoMessages() {
    String a = conv(0);
    String b = conv(0);
    String k = Ids.next();
    SequencedMessage ma = seq.process(a, List.of(cmd(a, k, "to a"))).getFirst();
    SequencedMessage mb = seq.process(b, List.of(cmd(b, k, "to b"))).getFirst();
    assertThat(ma.duplicate()).isFalse();
    assertThat(mb.duplicate()).isFalse();
    assertThat(ma.message().msgId()).isNotEqualTo(mb.message().msgId());
    assertThat(stores.messages.get(b).get(1L).text()).isEqualTo("to b");
  }

  @Test
  void fencedWhenSeqMovedElsewhere() {
    String c = conv(0);
    seq.process(c, List.of(cmd(c, Ids.next(), "a")));
    stores.lastSeq.put(c, 5L);   // another sequencer wrote meanwhile
    int before = stores.messages.get(c).size();
    assertThatThrownBy(() -> seq.process(c, List.of(cmd(c, Ids.next(), "b")))).isInstanceOf(FencedException.class);
    assertThat(stores.messages.get(c)).hasSize(before);
    // the retry re-reads last_seq
    assertThat(seq.process(c, List.of(cmd(c, Ids.next(), "b")))).extracting(m -> m.message().convSeq())
        .containsExactly(6L);
  }

  @Test
  void replyToIsCarried() {
    String c = conv(0);
    ChatCommand http = cmd(c, Ids.next(), "x").toBuilder().setReplyTo("http:pod-1").build();
    assertThat(seq.process(c, List.of(http)).getFirst().replyTo()).isEqualTo("http:pod-1");
  }

  /** Final review C1: a message write that fails after the seq was reserved must not make a second message. */
  @Test
  void messageWriteFailureIsRetriedAsTheSameMessage() {
    String c = conv(0);
    ChatCommand cmd = cmd(c, Ids.next(), "once");
    stores.failWrites = 1;
    assertThatThrownBy(() -> seq.process(c, List.of(cmd))).isInstanceOf(StoreUnavailableException.class);
    redis.clear();
    SequencedMessage m = seq.process(c, List.of(cmd)).getFirst();
    assertThat(m.message().convSeq()).isEqualTo(1);
    assertThat(stores.lastSeq.get(c)).as("no second seq").isEqualTo(1L);
    assertThat(stores.messages.get(c)).containsOnlyKeys(1L);
    assertThat(stores.messages.get(c).get(1L).text()).isEqualTo("once");
  }

  /** A retry carrying other text re-emits what is stored, not what it carries. */
  @Test
  void duplicateEventCarriesTheStoredMessage() {
    String c = conv(0);
    String k = Ids.next();
    seq.process(c, List.of(cmd(c, k, "original")));
    SequencedMessage again = seq.process(c, List.of(cmd(c, k, "edited"))).getFirst();
    assertThat(again.duplicate()).isTrue();
    assertThat(again.message().text()).isEqualTo("original");
  }
}
