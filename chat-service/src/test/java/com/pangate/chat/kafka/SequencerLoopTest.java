package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.SendResults;
import com.pangate.chat.core.Sequencer;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Perf 3c (spec §4.2): the sequencer runs one lane per conversation on the keyed pipeline. */
class SequencerLoopTest {

  final FakeStores stores = new FakeStores();
  final TopicPartition tp = new TopicPartition(ChatTopics.COMMANDS, 0);
  final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
  final MockProducer<String, byte[]> events = PoisonRecordTest.producer();
  final MockProducer<String, byte[]> dlq = PoisonRecordTest.producer();
  final AtomicLong offset = new AtomicLong();
  SequencerLoop loop;

  String conversation() {
    String c = Ids.next();
    stores.create(new Conversation(c, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
    return c;
  }

  void start() {
    loop = new SequencerLoop(consumer, ChatTopics.COMMANDS,
        new Sequencer(stores, stores, stores, stores, System::currentTimeMillis),
        new EventProducer(events, "chat.events", Duration.ofSeconds(1)), SendResults.NONE,
        new CommandProducer(PoisonRecordTest.producer(), Duration.ofSeconds(1), ChatTopics.COMMANDS),
        new SimpleMeterRegistry());
    consumer.schedulePollTask(() -> {
      consumer.rebalance(List.of(tp));
      consumer.updateBeginningOffsets(new HashMap<>(Map.of(tp, 0L)));
    });
    loop.start();
    await().until(() -> !consumer.assignment().isEmpty());
  }

  void send(String conv, String text) {
    ChatCommand c = ChatCommand.newBuilder().setSenderId("u1").setSenderDeviceId("d1").setReceivedAtMs(1)
        .setSend(ChatSend.newBuilder().setConversationId(conv).setClientMsgId(Ids.next()).setText(text)).build();
    consumer.addRecord(new ConsumerRecord<>(ChatTopics.COMMANDS, 0, offset.getAndIncrement(), conv, c.toByteArray()));
  }

  List<ChatEvent> eventsOf(String conv) {
    return events.history().stream().map(PoisonRecordTest::parse)
        .filter(e -> e.getMessage().getConversationId().equals(conv)).toList();
  }

  @AfterEach
  void stop() {
    if (loop != null) {
      loop.close();
    }
  }

  @Test
  void conversationsOfOnePartitionAreSequencedInParallel() {
    String c1 = conversation();
    String c2 = conversation();
    CountDownLatch hold = new CountDownLatch(1);
    stores.holdWrites.put(c1, hold);
    start();
    send(c1, "slow");
    send(c2, "fast");
    await().until(() -> eventsOf(c2).size() == 1);
    assertThat(eventsOf(c1)).isEmpty();
    hold.countDown();
    await().until(() -> eventsOf(c1).size() == 1);
  }

  @Test
  void hotConversationIsBatchedInOrder() {
    String c1 = conversation();
    start();
    for (int i = 0; i < 300; i++) {
      send(c1, Integer.toString(i));
    }
    await().until(() -> eventsOf(c1).size() == 300);
    List<ChatEvent> got = eventsOf(c1);
    for (int i = 0; i < 300; i++) {
      assertThat(got.get(i).getMessage().getConvSeq()).isEqualTo(i + 1);
      assertThat(got.get(i).getMessage().getText()).isEqualTo(Integer.toString(i));
    }
    assertThat(stores.reserveCalls).isBetween(3, 300);
  }

  @Test
  void fenceIsRetriedInPlace() {
    String c1 = conversation();
    start();
    send(c1, "first");
    await().until(() -> eventsOf(c1).size() == 1);
    stores.lastSeq.put(c1, 5L);   // another sequencer reserved 2..5 meanwhile
    send(c1, "second");
    await().until(() -> eventsOf(c1).size() == 2);
    assertThat(eventsOf(c1).get(1).getMessage().getConvSeq()).isEqualTo(6);
  }

  /** 3c minor: a malformed command in a batch that is retried is dead-lettered once, not on every try. */
  @Test
  void malformedCommandIsDeadLetteredOnceAcrossRetries() {
    String c1 = conversation();
    stores.failWrites = 2;
    consumer.schedulePollTask(() -> { });
    loop = new SequencerLoop(consumer, ChatTopics.COMMANDS,
        new Sequencer(stores, stores, stores, stores, System::currentTimeMillis),
        new EventProducer(events, "chat.events", Duration.ofSeconds(1)), SendResults.NONE,
        new CommandProducer(dlq, Duration.ofSeconds(1), ChatTopics.COMMANDS), new SimpleMeterRegistry());
    consumer.schedulePollTask(() -> {
      consumer.rebalance(List.of(tp));
      consumer.updateBeginningOffsets(new HashMap<>(Map.of(tp, 0L)));
      consumer.addRecord(new ConsumerRecord<>(ChatTopics.COMMANDS, 0, offset.getAndIncrement(), c1,
          new byte[] {(byte) 0xff, 1}));
      send(c1, "ok");
    });
    loop.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> eventsOf(c1).size() == 1);
    assertThat(dlq.history()).hasSize(1);
  }
}
