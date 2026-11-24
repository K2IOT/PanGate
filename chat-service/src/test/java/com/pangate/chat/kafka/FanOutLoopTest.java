package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.core.Pusher;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushResponse;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FanOutLoopTest {

  final FakeStores stores = new FakeStores();
  final TopicPartition tp = new TopicPartition(ChatTopics.EVENTS, 0);
  final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
  final AtomicInteger pushes = new AtomicInteger();
  final Set<String> silent = java.util.concurrent.ConcurrentHashMap.newKeySet();
  final Pusher pusher = (r, p) -> {
    pushes.incrementAndGet();
    if (silent.contains(r.getPrincipalId())) {
      return new CompletableFuture<>();   // a Push API that never answers for this user
    }
    return CompletableFuture.completedFuture(PushResponse.newBuilder().setRequestId(r.getRequestId())
        .setOk(true).setCode(PushResponse.Code.CODE_OK).build());
  };
  FanOutLoop loop;
  long offset;

  @Test
  void consumerTakesSmallPollsAndCommitsByHand() {
    var cfg = FanOutLoop.consumerConfig("k:9092", "g");
    assertThat(cfg.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
    assertThat(cfg.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
  }

  String group(String prefix, int size) {
    String c = Ids.next();
    List<Member> ms = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      ms.add(new Member(prefix + i, i == 0 ? Role.OWNER : Role.MEMBER));
    }
    stores.create(new Conversation(c, ConversationKind.CONVERSATION_KIND_GROUP, "g", prefix + "0", 0, false, size),
        ms);
    return c;
  }

  void start() {
    loop = new FanOutLoop(consumer, ChatTopics.EVENTS, new MemberLookup(stores, stores), stores, stores, pusher,
        new SimpleMeterRegistry());
    consumer.schedulePollTask(() -> {
      consumer.rebalance(List.of(tp));
      consumer.updateBeginningOffsets(new HashMap<>(Map.of(tp, 0L)));
    });
    loop.start();
    await().until(() -> !consumer.assignment().isEmpty());
  }

  void event(String conv, String sender, long seq) {
    ChatEvent e = ChatEvent.newBuilder().setKind(ConversationKind.CONVERSATION_KIND_GROUP)
        .setMessage(ChatMessage.newBuilder().setConversationId(conv).setMsgId(Ids.next()).setConvSeq(seq)
            .setSenderId(sender).setText("hi").setSentAtMs(System.currentTimeMillis())).build();
    consumer.addRecord(new ConsumerRecord<>(ChatTopics.EVENTS, 0, offset++, conv, e.toByteArray()));
  }

  long inboxSeq(String user, String conv) {
    var row = stores.userConvs.getOrDefault(user, Map.of()).get(conv);
    return row == null ? 0 : row.lastSeq();
  }

  @AfterEach
  void stop() {
    if (loop != null) {
      loop.close();
    }
  }

  /** Perf 3c (spec §4.3): one call for every member's inbox row; only those that failed are written again. */
  @Test
  void inboxOf500IsWrittenTogetherAndOnlyFailuresRetried() {
    String g = group("m", 500);
    stores.failTouchFor.putAll(Map.of("m1", 1, "m2", 1, "m3", 1));
    start();
    event(g, "m0", 1);
    await().until(() -> inboxSeq("m3", g) == 1 && inboxSeq("m499", g) == 1);
    assertThat(stores.touchBatches.getFirst()).hasSize(500);
    assertThat(stores.touchBatches.get(1)).containsExactlyInAnyOrder("m1", "m2", "m3");
    assertThat(pushes.get()).as("one push per member, the sender's other devices included, none resent").isEqualTo(500);
  }

  /** Perf 3c (spec §2): a conversation whose pushes never answer does not hold another one on the same partition. */
  @Test
  void slowConversationDoesNotDelayAnotherOnTheSamePartition() {
    String slow = group("s", 3);
    String fast = group("f", 3);
    silent.add(com.pangate.chat.core.UserIds.principal("s1"));
    start();
    event(slow, "s0", 1);
    event(fast, "f0", 1);
    await().until(() -> inboxSeq("f1", fast) == 1);
    assertThat(inboxSeq("s1", slow)).isZero();
    await().until(() -> {
      OffsetAndMetadata c = consumer.committed(Set.of(tp)).get(tp);
      return c != null;
    });
    assertThat(consumer.committed(Set.of(tp)).get(tp).offset()).isZero();
  }
}
