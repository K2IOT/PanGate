package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Marks;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.core.PrivacyCache;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.ReceiptCommand;
import com.pangate.contracts.v1.ReceiptUpdate;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** CH3 spec §3.4: chat.receipts → marks in the store (never back) → receipt pushes. */
class ReceiptWorkerIT {

  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  static final ChatProperties.State STATE = new ChatProperties.State(50, 100, Duration.ofMillis(500),
      Duration.ofSeconds(2), 50, Duration.ofSeconds(5), Duration.ofMinutes(5), 200, Duration.ofSeconds(2));

  @BeforeAll
  static void start() {
    kafka.start();
  }

  @AfterAll
  static void stop() {
    kafka.stop();
  }

  static String topic() throws Exception {
    String t = "rc-" + Ids.next();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(t, 2, (short) 1))).all().get();
    }
    return t;
  }

  record Rig(FakeStores stores, List<PushRequest> pushed, ReceiptWorkerLoop loop) implements AutoCloseable {
    @Override
    public void close() {
      loop.close();
    }
  }

  static Rig rig(String topic) {
    FakeStores stores = new FakeStores();
    List<PushRequest> pushed = new CopyOnWriteArrayList<>();
    ReceiptWorkerLoop loop = new ReceiptWorkerLoop(ReceiptWorkerLoop.consumer(kafka.getBootstrapServers(),
        "rw-" + topic), topic, stores, new MemberLookup(stores, stores), stores, new PrivacyCache(stores,
        System::currentTimeMillis), r -> {
          pushed.add(r);
          return CompletableFuture.completedFuture(PushResponse.getDefaultInstance());
        }, STATE, new SimpleMeterRegistry());
    return new Rig(stores, pushed, loop);
  }

  static void send(String topic, ReceiptCommand... cs) {
    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
      for (ReceiptCommand c : cs) {
        p.send(new ProducerRecord<>(topic, c.getConversationId(), c.toByteArray()));
      }
      p.flush();
    }
  }

  static ReceiptCommand read(String conv, String user, long seq) {
    return ReceiptCommand.newBuilder().setConversationId(conv).setUserId(user).setDeviceId("d1")
        .setDeliveredSeq(seq).setReadSeq(seq).setReceivedAtMs(System.currentTimeMillis()).build();
  }

  static String conv(FakeStores s, ConversationKind kind, String... users) {
    String id = Ids.next();
    s.create(new Conversation(id, kind, "t", users[0], 0, false, users.length),
        java.util.Arrays.stream(users).map(u -> new Member(u, Role.MEMBER)).toList());
    return id;
  }

  /** Bất biến 1: late, smaller and replayed marks never move a mark back. */
  @Test
  void outOfOrderAndReplayNeverRegress() throws Exception {
    String t = topic();
    try (Rig r = rig(t)) {
      String g = conv(r.stores(), ConversationKind.CONVERSATION_KIND_GROUP, "a", "b", "c");
      r.loop().start();
      send(t, read(g, "b", 9), read(g, "b", 4));
      await().atMost(Duration.ofSeconds(20)).until(() -> r.stores().own("b", g).equals(new Marks(9, 9)));
      send(t, read(g, "b", 2), read(g, "b", 9));
      Thread.sleep(3_000);
      assertThat(r.stores().own("b", g)).isEqualTo(new Marks(9, 9));
      assertThat(r.stores().marks(g)).contains(new com.pangate.chat.core.ChatStores.MemberMarks("b", 9, 9));
    }
  }

  @Test
  void dmPeerGetsReceiptWithinASecond() throws Exception {
    String t = topic();
    try (Rig r = rig(t)) {
      String d = conv(r.stores(), ConversationKind.CONVERSATION_KIND_DIRECT, "a", "b");
      r.loop().start();
      Thread.sleep(2_000);   // assigned
      long t0 = System.currentTimeMillis();
      send(t, read(d, "b", 3));
      await().atMost(Duration.ofSeconds(5)).until(() -> r.pushed().stream()
          .anyMatch(p -> p.getPrincipalId().equals("chat:a")));
      assertThat(System.currentTimeMillis() - t0).isLessThan(1_500);
      PushRequest toA = r.pushed().stream().filter(p -> p.getPrincipalId().equals("chat:a")).findFirst().orElseThrow();
      assertThat(toA.getHeadersMap()).containsEntry("type", ChatTopics.TYPE_RECEIPTS);
      assertThat(ReceiptUpdate.parseFrom(toA.getPayload()).getMarks(0).getReadSeq()).isEqualTo(3);
      // the DM peer's inbox row is stamped, so its /v1/state shows the change (spec §7.2)
      assertThat(r.stores().stateAt).containsKey("a|" + d);
    }
  }

  @Test
  void storeOutageIsRetriedWithoutLosingMarks() throws Exception {
    String t = topic();
    try (Rig r = rig(t)) {
      String g = conv(r.stores(), ConversationKind.CONVERSATION_KIND_GROUP, "a", "b");
      r.stores().fail = true;
      r.loop().start();
      send(t, read(g, "b", 5));
      Thread.sleep(3_000);
      r.stores().fail = false;
      await().atMost(Duration.ofSeconds(20)).until(() -> r.stores().own("b", g).equals(new Marks(5, 5)));
    }
  }
}
