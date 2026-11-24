package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChannelSubscriptions;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.StateCommands;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ChatTyping;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PresenceWatch;
import com.pangate.contracts.v1.UpstreamRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Perf 3c (spec §4.4): chat-ingest takes sends and channel ops, chat-state takes the state ops. */
class UpstreamCommandListenerTest {

  static final String TOPIC = "c1.gw.upstream.cmd";
  final FakeStores stores = new FakeStores();
  final String conv = Ids.next();
  final List<String> published = Collections.synchronizedList(new ArrayList<>());
  final List<String> stateCalls = Collections.synchronizedList(new ArrayList<>());
  final List<String> outcomes = Collections.synchronizedList(new ArrayList<>());
  final CountDownLatch watchRelease = new CountDownLatch(1);
  final List<UpstreamCommandListener> made = new ArrayList<>();

  final StateCommands state = new StateCommands() {
    @Override
    public String mark(String userId, String deviceId, ChatMark m, boolean read) {
      stateCalls.add(read ? "read" : "delivered");
      return "accepted";
    }

    @Override
    public String typing(String userId, String deviceId, ChatTyping t) {
      stateCalls.add("typing");
      return "accepted";
    }

    @Override
    public String watch(String userId, String deviceId, PresenceWatch w) {
      stateCalls.add("watch");
      try {
        watchRelease.await();   // a slow watch
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "accepted";
    }
  };

  UpstreamCommandListenerTest() {
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
  }

  MockConsumer<String, byte[]> consumerWith(List<byte[]> values) {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    c.schedulePollTask(() -> {
      c.rebalance(List.of(tp));
      c.updateBeginningOffsets(new HashMap<>(Map.of(tp, 0L)));
      for (int i = 0; i < values.size(); i++) {
        c.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "chat:u1", values.get(i)));
      }
    });
    return c;
  }

  UpstreamCommandListener listener(MockConsumer<String, byte[]> c, UpstreamCommandListener.Role role) {
    Ingest ingest = new Ingest(stores, stores, stores, cmd -> published.add(cmd.getSend().getClientMsgId()),
        FakeStores.LIMITS, System::nanoTime, System::currentTimeMillis);
    UpstreamCommandListener l = new UpstreamCommandListener(c, TOPIC, ingest, (sender, r) -> { },
        new CommandProducer(PoisonRecordTest.producer(), Duration.ofSeconds(1), ChatTopics.COMMANDS), outcomes::add,
        new ChannelSubscriptions(stores, stores, (r, prio) -> new CompletableFuture<>()), state, role);
    made.add(l);
    l.start();
    return l;
  }

  static byte[] op(String op, com.google.protobuf.MessageLite payload) {
    return UpstreamRecord.newBuilder().setTenantId(ChatTopics.TENANT).setPrincipalId("chat:u1").setDeviceId("d1")
        .putHeaders("op", op).setPayload(payload.toByteString()).build().toByteArray();
  }

  byte[] send(String clientMsgId) {
    return op(UpstreamCommandListener.OP_SEND, ChatSend.newBuilder().setConversationId(conv)
        .setClientMsgId(clientMsgId).setText("hi").build());
  }

  byte[] read() {
    return op(ChatTopics.OP_READ, ChatMark.newBuilder().setConversationId(conv).setUpToSeq(1).build());
  }

  byte[] watch() {
    return op(ChatTopics.OP_WATCH, PresenceWatch.newBuilder().build());
  }

  @AfterEach
  void stop() {
    watchRelease.countDown();
    made.forEach(UpstreamCommandListener::close);
  }

  @Test
  void ingestIgnoresStateOps() {
    String id = Ids.next();
    listener(consumerWith(List.of(read(), send(id))), UpstreamCommandListener.Role.INGEST);
    await().until(() -> published.contains(id));
    await().until(() -> outcomes.size() == 2);
    assertThat(stateCalls).isEmpty();
    assertThat(outcomes).containsExactly("ignored", "accepted");
  }

  @Test
  void stateIgnoresSendAndChannelOps() {
    String id = Ids.next();
    listener(consumerWith(List.of(send(id), read())), UpstreamCommandListener.Role.STATE);
    await().until(() -> stateCalls.contains("read"));
    await().until(() -> outcomes.size() == 2);
    assertThat(published).isEmpty();
    assertThat(outcomes.getFirst()).isEqualTo("ignored");
  }

  @Test
  void slowWatchDoesNotDelaySendOfTheSameUser() {
    String id = Ids.next();
    List<byte[]> records = List.of(watch(), send(id));
    listener(consumerWith(records), UpstreamCommandListener.Role.STATE);
    listener(consumerWith(records), UpstreamCommandListener.Role.INGEST);
    await().until(() -> stateCalls.contains("watch"));
    await().until(() -> published.contains(id));
  }
}
