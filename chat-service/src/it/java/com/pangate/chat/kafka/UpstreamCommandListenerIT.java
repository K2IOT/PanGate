package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.Ingest;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.UpstreamRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class UpstreamCommandListenerIT {

  static final String UPSTREAM = "c1.gw.upstream.cmd";
  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

  @BeforeAll
  static void start() throws Exception {
    kafka.start();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(UPSTREAM, 4, (short) 1), new NewTopic(ChatTopics.COMMANDS, 4, (short) 1),
          new NewTopic(ChatTopics.DLQ, 1, (short) 1))).all().get();
    }
  }

  @AfterAll
  static void stop() {
    kafka.stop();
  }

  static byte[] upstream(String tenant, String principal, String op, byte[] payload) {
    return UpstreamRecord.newBuilder().setMsgId(Ids.next()).setTenantId(tenant).setPrincipalId(principal)
        .setDeviceId("d1").setCellId("c1").putHeaders("op", op).setPayload(com.google.protobuf.ByteString
            .copyFrom(payload)).build().toByteArray();
  }

  static List<ConsumerRecord<String, byte[]>> drain(String topic, int atLeast) {
    List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers(), "group.id", "check-" + topic + Ids.next(), "auto.offset.reset", "earliest"),
        new StringDeserializer(), new ByteArrayDeserializer())) {
      c.subscribe(List.of(topic));
      long deadline = System.currentTimeMillis() + 30_000;
      while (out.size() < atLeast && System.currentTimeMillis() < deadline) {
        c.poll(Duration.ofMillis(200)).forEach(out::add);
      }
      c.poll(Duration.ofMillis(500)).forEach(out::add);
    }
    return out;
  }

  @Test
  void chatSendFromUpstreamBecomesACommand() throws Exception {
    FakeStores stores = new FakeStores();
    String conv = Ids.next();
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
    List<ChatRejected> rejected = new CopyOnWriteArrayList<>();
    try (CommandProducer producer = CommandProducer.create(kafka.getBootstrapServers(), Duration.ofSeconds(5));
        UpstreamCommandListener listener = new UpstreamCommandListener(UpstreamCommandListener.consumer(
            kafka.getBootstrapServers()), UPSTREAM,
            new Ingest(stores, stores, stores, producer, FakeStores.LIMITS, System::nanoTime,
                System::currentTimeMillis),
            (sender, r) -> rejected.add(r), producer, outcome -> { })) {
      listener.start();
      ChatSend ok = ChatSend.newBuilder().setConversationId(conv).setClientMsgId(Ids.next()).setText("hi").build();
      ChatSend notMine = ChatSend.newBuilder().setConversationId(Ids.next()).setClientMsgId(Ids.next())
          .setText("hi").build();
      try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
          kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
        p.send(new ProducerRecord<>(UPSTREAM, "chat:u1", upstream("chat", "chat:u1", "chat.send", ok.toByteArray())));
        p.send(new ProducerRecord<>(UPSTREAM, "chat:u1", upstream("chat", "chat:u1", "other", ok.toByteArray())));
        p.send(new ProducerRecord<>(UPSTREAM, "x", upstream("t1", "x", "chat.send", ok.toByteArray())));
        p.send(new ProducerRecord<>(UPSTREAM, "chat:u1", upstream("chat", "chat:u1", "chat.send",
            new byte[] {(byte) 0xff, 1, 2})));
        p.send(new ProducerRecord<>(UPSTREAM, "chat:u1", new byte[] {(byte) 0xff, 0x0f}));
        p.send(new ProducerRecord<>(UPSTREAM, "chat:u1", upstream("chat", "chat:u1", "chat.send",
            notMine.toByteArray())));
        p.flush();
      }
      List<ConsumerRecord<String, byte[]>> cmds = drain(ChatTopics.COMMANDS, 1);
      assertThat(cmds).singleElement().satisfies(r -> {
        assertThat(r.key()).isEqualTo(conv);
        ChatCommand c = ChatCommand.parseFrom(r.value());
        assertThat(c.getSenderId()).isEqualTo("u1");
        assertThat(c.getSenderDeviceId()).isEqualTo("d1");
        assertThat(c.getSend()).isEqualTo(ok);
      });
      assertThat(drain(ChatTopics.DLQ, 2)).hasSize(2);
      await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(rejected).singleElement()
          .satisfies(r -> {
            assertThat(r.getReason()).isEqualTo("not_found");
            assertThat(r.getClientMsgId()).isEqualTo(notMine.getClientMsgId());
          }));
    }
  }
}
