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
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChannelClose;
import com.pangate.contracts.v1.ChannelOpen;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.UpstreamRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
            (sender, r) -> rejected.add(r), producer, outcome -> { },
            new ChannelSubscriptions(stores, stores, r -> new CompletableFuture<>()))) {
      listener.start();
      ChatSend ok = ChatSend.newBuilder().setConversationId(conv).setClientMsgId(Ids.next()).setText("hi").build();
      ChatSend notMine = ChatSend.newBuilder().setConversationId(Ids.next()).setClientMsgId(Ids.next())
          .setText("hi").build();
      try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
          kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
        // another tenant's record that does not parse is not ours to dead-letter (key = its principal)
        p.send(new ProducerRecord<>(UPSTREAM, "t1:x", new byte[] {(byte) 0xff, 0x0f}));
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
      assertThat(drain(ChatTopics.DLQ, 2)).hasSize(2).allSatisfy(r -> assertThat(r.key()).isEqualTo("chat:u1"));
      await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(rejected).singleElement()
          .satisfies(r -> {
            assertThat(r.getReason()).isEqualTo("not_found");
            assertThat(r.getClientMsgId()).isEqualTo(notMine.getClientMsgId());
          }));
    }
  }

  @Test
  void channelOpenAndCloseBecomeServerSideSubscriptions() throws Exception {
    String upstream = "c1.gw.upstream.ch";
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(upstream, 1, (short) 1))).all().get();
    }
    FakeStores stores = new FakeStores();
    String channel = Ids.next();
    stores.create(new Conversation(channel, ConversationKind.CONVERSATION_KIND_CHANNEL, "c", "own", 0, true, 1),
        List.of(new Member("own", Role.OWNER)));
    String group = Ids.next();
    stores.create(new Conversation(group, ConversationKind.CONVERSATION_KIND_GROUP, "g", "own", 0, false, 1),
        List.of(new Member("own", Role.OWNER)));
    List<PushRequest> controls = new CopyOnWriteArrayList<>();
    List<ChatRejected> rejected = new CopyOnWriteArrayList<>();
    try (CommandProducer producer = CommandProducer.create(kafka.getBootstrapServers(), Duration.ofSeconds(5));
        UpstreamCommandListener listener = new UpstreamCommandListener(consumerFor(kafka.getBootstrapServers()),
            upstream, new Ingest(stores, stores, stores, producer, FakeStores.LIMITS, System::nanoTime,
                System::currentTimeMillis), (sender, r) -> rejected.add(r), producer, outcome -> { },
            new ChannelSubscriptions(stores, stores, r -> {
              controls.add(r);
              return new CompletableFuture<>();
            }))) {
      listener.start();
      try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
          kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
        p.send(new ProducerRecord<>(upstream, "chat:fan", upstream("chat", "chat:fan", "chat.channel.open",
            ChannelOpen.newBuilder().setConversationId(channel).build().toByteArray())));
        p.send(new ProducerRecord<>(upstream, "chat:fan", upstream("chat", "chat:fan", "chat.channel.close",
            ChannelClose.newBuilder().setConversationId(channel).build().toByteArray())));
        p.send(new ProducerRecord<>(upstream, "chat:fan", upstream("chat", "chat:fan", "chat.channel.open",
            ChannelOpen.newBuilder().setConversationId(group).build().toByteArray())));
        p.flush();
      }
      await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
        assertThat(controls).hasSize(2);
        assertThat(rejected).singleElement().satisfies(r -> {
          assertThat(r.getConversationId()).isEqualTo(group);
          assertThat(r.getReason()).isEqualTo("invalid");
        });
      });
      assertThat(controls.get(0).getPrincipalId()).isEqualTo("chat:fan");
      assertThat(controls.get(0).getControl().getSubscribe().getTopicsList()).containsExactly("chat:chan:" + channel);
      assertThat(controls.get(1).getControl().getUnsubscribe().getTopicsList())
          .containsExactly("chat:chan:" + channel);
    }
  }

  /** Its own group: the other test's listener must not take these partitions. */
  static KafkaConsumer<String, byte[]> consumerFor(String bootstrap) {
    return new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap, "group.id", "ingest-" + Ids.next(),
        "enable.auto.commit", false, "auto.offset.reset", "earliest"), new StringDeserializer(),
        new ByteArrayDeserializer());
  }

  /** Upstream partitions are independent: a slow command sink in one does not hold the others. */
  @Test
  void partitionsAreIngestedInParallel() throws Exception {
    String upstream = "c1.gw.upstream.par";
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(upstream, 8, (short) 1))).all().get();
    }
    FakeStores stores = new FakeStores();
    List<String> users = new ArrayList<>();
    List<String> convs = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      String c = Ids.next();
      stores.create(new Conversation(c, ConversationKind.CONVERSATION_KIND_DIRECT, "", "p" + i, 0, false, 1),
          List.of(new Member("p" + i, Role.MEMBER)));
      users.add("p" + i);
      convs.add(c);
    }
    List<Long> published = new CopyOnWriteArrayList<>();
    com.pangate.chat.core.CommandSink slow = cmd -> {
      try {
        Thread.sleep(300);   // every command takes 300 ms to be acked
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      published.add(System.nanoTime());
    };
    try (CommandProducer producer = CommandProducer.create(kafka.getBootstrapServers(), Duration.ofSeconds(5));
        UpstreamCommandListener listener = new UpstreamCommandListener(consumerFor(kafka.getBootstrapServers()),
            upstream, new Ingest(stores, stores, stores, slow, FakeStores.LIMITS, System::nanoTime,
                System::currentTimeMillis), (sender, r) -> { }, producer, outcome -> { },
            new ChannelSubscriptions(stores, stores, r -> new CompletableFuture<>()))) {
      listener.start();
      Thread.sleep(3_000);   // joined the group
      long t0 = System.nanoTime();
      try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
          kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
        for (int i = 0; i < 16; i++) {
          p.send(new ProducerRecord<>(upstream, "chat:" + users.get(i), upstream("chat", "chat:" + users.get(i),
              "chat.send", ChatSend.newBuilder().setConversationId(convs.get(i)).setClientMsgId(Ids.next())
                  .setText("x").build().toByteArray())));
        }
        p.flush();
      }
      await().atMost(Duration.ofSeconds(20)).until(() -> published.size() == 16);
      long took = (published.stream().mapToLong(Long::longValue).max().orElseThrow() - t0) / 1_000_000;
      // 16 commands, 300 ms each: one thread needs ≥ 4.8 s
      assertThat(took).as("ms for 16 commands").isLessThan(2_500);
    }
  }
}
