package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChannelSubscriptions;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.SendResults;
import com.pangate.chat.core.Sequencer;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.UpstreamRecord;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * Final review minor: a record that fails the same deterministic way every time must not hold its partition forever.
 * After {@link PoisonGuard#MAX_ATTEMPTS} tries it goes to {@code chat.commands.dlq} and the partition moves on.
 */
class PoisonRecordTest {

  final FakeStores stores = new FakeStores();
  final String conv = Ids.next();

  PoisonRecordTest() {
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
  }

  static MockProducer<String, byte[]> producer() {
    return new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
  }

  static MockConsumer<String, byte[]> consumer(String topic, List<byte[]> values, String key) {
    TopicPartition tp = new TopicPartition(topic, 0);
    // a real consumer reads a partition again after seek; MockConsumer only does when the records are added again
    MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized void seek(TopicPartition p, long offset) {
        super.seek(p, offset);
        for (int i = (int) offset; i < values.size(); i++) {
          addRecord(new ConsumerRecord<>(topic, 0, i, key, values.get(i)));
        }
      }
    };
    c.schedulePollTask(() -> {
      c.rebalance(List.of(tp));
      c.updateBeginningOffsets(new HashMap<>(java.util.Map.of(tp, 0L)));
      for (int i = 0; i < values.size(); i++) {
        c.addRecord(new ConsumerRecord<>(topic, 0, i, key, values.get(i)));
      }
    });
    return c;
  }

  ChatSend send(String clientMsgId) {
    return ChatSend.newBuilder().setConversationId(conv).setClientMsgId(clientMsgId).setText("hi").build();
  }

  static String reason(ProducerRecord<String, byte[]> r) {
    return new String(r.headers().lastHeader("reason").value(), StandardCharsets.UTF_8);
  }

  @Test
  void sequencerDeadLettersOnlyThePoisonCommandAndGoesOn() {
    String good1 = Ids.next();
    String poison = Ids.next();
    String good2 = Ids.next();
    stores.poisonClientMsgId = poison;
    List<byte[]> values = new ArrayList<>();
    for (String k : List.of(good1, poison, good2)) {
      values.add(ChatCommand.newBuilder().setSend(send(k)).setSenderId("u1").setSenderDeviceId("d1")
          .setReceivedAtMs(1).build().toByteArray());
    }
    MockConsumer<String, byte[]> consumer = consumer(ChatTopics.COMMANDS, values, conv);
    MockProducer<String, byte[]> events = producer();
    MockProducer<String, byte[]> dlq = producer();
    try (SequencerLoop loop = new SequencerLoop(consumer, ChatTopics.COMMANDS,
        new Sequencer(stores, stores, stores, stores, System::currentTimeMillis),
        new EventProducer(events, "chat.events", Duration.ofSeconds(1)), SendResults.NONE,
        new CommandProducer(dlq, Duration.ofSeconds(1), ChatTopics.COMMANDS), new SimpleMeterRegistry())) {
      loop.start();
      await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
        assertThat(dlq.history()).singleElement().satisfies(r -> {
          assertThat(ChatCommand.parseFrom(r.value()).getSend().getClientMsgId()).isEqualTo(poison);
          assertThat(reason(r)).startsWith("poison");
        });
        assertThat(events.history().stream().map(r -> parse(r).getMessage().getClientMsgId()).distinct())
            .containsExactlyInAnyOrder(good1, good2);
      });
    }
  }

  @Test
  void ingestDeadLettersThePoisonRecordAndGoesOn() {
    String poison = Ids.next();
    List<String> published = Collections.synchronizedList(new ArrayList<>());
    Ingest ingest = new Ingest(stores, stores, stores, c -> {
      if (c.getSend().getClientMsgId().equals(poison)) {
        throw new IllegalArgumentException("cannot publish");
      }
      published.add(c.getSend().getClientMsgId());
    }, FakeStores.LIMITS, System::nanoTime, System::currentTimeMillis);
    String good1 = Ids.next();
    String good2 = Ids.next();
    List<byte[]> values = new ArrayList<>();
    for (String k : List.of(good1, poison, good2)) {
      values.add(UpstreamRecord.newBuilder().setTenantId(ChatTopics.TENANT).setPrincipalId("chat:u1")
          .setDeviceId("d1").putHeaders("op", UpstreamCommandListener.OP_SEND)
          .setPayload(send(k).toByteString()).build().toByteArray());
    }
    MockConsumer<String, byte[]> consumer = consumer("c1.gw.upstream.cmd", values, "chat:u1");
    MockProducer<String, byte[]> dlq = producer();
    try (UpstreamCommandListener l = new UpstreamCommandListener(consumer, "c1.gw.upstream.cmd", ingest,
        (sender, r) -> { }, new CommandProducer(dlq, Duration.ofSeconds(1), ChatTopics.COMMANDS), o -> { },
        new ChannelSubscriptions(stores, stores, r -> new CompletableFuture<>()))) {
      l.start();
      await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
        assertThat(dlq.history()).singleElement().satisfies(r -> assertThat(reason(r)).startsWith("poison"));
        assertThat(published).containsExactly(good1, good2);
      });
    }
  }

  static ChatEvent parse(ProducerRecord<String, byte[]> r) {
    try {
      return ChatEvent.parseFrom(r.value());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }
}
