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
      Duration.ofSeconds(2), 50, Duration.ofSeconds(5), Duration.ofMinutes(5), 200, Duration.ofSeconds(2), 5, 10, 1, 5);

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
    return rig(topic, com.pangate.chat.core.BadgeUpdates.NONE);
  }

  static Rig rig(String topic, com.pangate.chat.core.BadgeUpdates badges) {
    FakeStores stores = new FakeStores();
    List<PushRequest> pushed = new CopyOnWriteArrayList<>();
    ReceiptWorkerLoop loop = new ReceiptWorkerLoop(ReceiptWorkerLoop.consumer(kafka.getBootstrapServers(),
        "rw-" + topic), topic, stores, new MemberLookup(stores, stores), stores, new PrivacyCache(stores,
        System::currentTimeMillis), r -> {
          pushed.add(r);
          return CompletableFuture.completedFuture(PushResponse.getDefaultInstance());
        }, STATE, new SimpleMeterRegistry(), badges);
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

  /** Final review I4: a Push API that never answers costs one deadline per flush, not one per request. */
  @Test
  void hungPushApiDoesNotStallOtherConversations() throws Exception {
    String t = topic();
    FakeStores stores = new FakeStores();
    ReceiptWorkerLoop loop = new ReceiptWorkerLoop(ReceiptWorkerLoop.consumer(kafka.getBootstrapServers(),
        "rw-" + t), t, stores, new MemberLookup(stores, stores), stores, new PrivacyCache(stores,
        System::currentTimeMillis), r -> new CompletableFuture<>(), STATE, new SimpleMeterRegistry());
    try (loop) {
      List<String> groups = new java.util.ArrayList<>();
      for (int i = 0; i < 3; i++) {
        String[] users = new String[20];
        for (int k = 0; k < 20; k++) {
          users[k] = "h" + i + "_" + k;
        }
        groups.add(conv(stores, ConversationKind.CONVERSATION_KIND_GROUP, users));
      }
      loop.start();
      for (String g : groups) {
        send(t, read(g, "h" + groups.indexOf(g) + "_1", 3));
      }
      // serially that is 3 flushes x 20 requests x 3 tries x 5 s; concurrently a few deadlines
      await().atMost(Duration.ofSeconds(40)).until(() -> groups.stream().allMatch(g ->
          stores.own("h" + groups.indexOf(g) + "_1", g).equals(new Marks(3, 3))));
    }
  }

  static String singlePartitionTopic() throws Exception {
    String t = "rc1-" + Ids.next();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(t, 1, (short) 1))).all().get();
    }
    return t;
  }

  /**
   * Review of PR #6: a Push API that never answers costs one set of push rounds per step, not one per conversation,
   * so the marks of every conversation due together are written at once and poll() is not starved.
   */
  @Test
  void hungPushApiCostsOneRoundSetPerStep() throws Exception {
    String t = singlePartitionTopic();
    FakeStores stores = new FakeStores();
    ReceiptWorkerLoop loop = new ReceiptWorkerLoop(ReceiptWorkerLoop.consumer(kafka.getBootstrapServers(),
        "rw-" + t), t, stores, new MemberLookup(stores, stores), stores, new PrivacyCache(stores,
        System::currentTimeMillis), r -> new CompletableFuture<>(), STATE, new SimpleMeterRegistry());
    try (loop) {
      List<String> groups = new java.util.ArrayList<>();
      for (int i = 0; i < 8; i++) {
        groups.add(conv(stores, ConversationKind.CONVERSATION_KIND_GROUP, "q" + i + "a", "q" + i + "b"));
      }
      loop.start();
      Thread.sleep(2_000);   // assigned
      send(t, groups.stream().map(g -> read(g, "q" + groups.indexOf(g) + "b", 3)).toArray(ReceiptCommand[]::new));
      // one flush after another that is 8 x 3 rounds x 3 s before the last marks are written
      await().atMost(Duration.ofSeconds(20)).until(() -> groups.stream().allMatch(g ->
          stores.own("q" + groups.indexOf(g) + "b", g).equals(new Marks(3, 3))));
    }
  }

  /** Review of PR #6: a store failure part-way through a step does not push the earlier conversations twice. */
  @Test
  void storeFailureMidStepDoesNotRepeatPushes() throws Exception {
    String t = singlePartitionTopic();
    try (Rig r = rig(t)) {
      List<String> groups = new java.util.ArrayList<>();
      for (int i = 0; i < 6; i++) {
        groups.add(conv(r.stores(), ConversationKind.CONVERSATION_KIND_GROUP, "p" + i + "a", "p" + i + "b"));
      }
      // the aggregator iterates a HashMap: fail the conversation it flushes last, after the others were handled
      java.util.Map<String, Boolean> order = new java.util.HashMap<>();
      groups.forEach(g -> order.put(g, true));
      String last = null;
      for (String g : order.keySet()) {
        last = g;
      }
      r.stores().failAdvanceFor.put(last, 2);
      r.loop().start();
      Thread.sleep(2_000);   // assigned
      send(t, groups.stream().map(g -> read(g, "p" + groups.indexOf(g) + "b", 3)).toArray(ReceiptCommand[]::new));
      await().atMost(Duration.ofSeconds(30)).until(() -> groups.stream().allMatch(g ->
          r.stores().own("p" + groups.indexOf(g) + "b", g).equals(new Marks(3, 3))));
      Thread.sleep(1_000);
      for (String g : groups) {
        String peer = "chat:p" + groups.indexOf(g) + "a";
        assertThat(r.pushed().stream().filter(p -> p.getPrincipalId().equals(peer)).count())
            .as("receipt pushes to %s", peer).isEqualTo(1);
      }
    }
  }

  /** Codex review of PR #6: a rate-limited push is retried after the Push API's retry_after_ms, not at once. */
  @Test
  void rateLimitedPushWaitsRetryAfter() throws Exception {
    String t = singlePartitionTopic();
    FakeStores stores = new FakeStores();
    List<Long> attempts = new CopyOnWriteArrayList<>();
    ReceiptWorkerLoop loop = new ReceiptWorkerLoop(ReceiptWorkerLoop.consumer(kafka.getBootstrapServers(),
        "rw-" + t), t, stores, new MemberLookup(stores, stores), stores, new PrivacyCache(stores,
        System::currentTimeMillis), r -> {
          if (!r.getPrincipalId().equals("chat:ra")) {
            return CompletableFuture.completedFuture(PushResponse.getDefaultInstance());
          }
          attempts.add(System.nanoTime());
          return CompletableFuture.completedFuture(attempts.size() == 1
              ? PushResponse.newBuilder().setCode(PushResponse.Code.CODE_RATE_LIMITED).setRetryAfterMs(1_500).build()
              : PushResponse.getDefaultInstance());
        }, STATE, new SimpleMeterRegistry());
    try (loop) {
      String d = conv(stores, ConversationKind.CONVERSATION_KIND_DIRECT, "ra", "rb");
      loop.start();
      Thread.sleep(2_000);   // assigned
      send(t, read(d, "rb", 2));
      await().atMost(Duration.ofSeconds(15)).until(() -> attempts.size() >= 2);
      assertThat((attempts.get(1) - attempts.get(0)) / 1_000_000).isGreaterThanOrEqualTo(1_400);
    }
  }

  /** Perf 3a (spec §3.3): a read mark of a DM or group raises the reader's counter after the mark is written. */
  @Test
  void readRaisesBadge() throws Exception {
    String t = topic();
    com.pangate.chat.core.RecordingBadges b = new com.pangate.chat.core.RecordingBadges();
    try (Rig r = rig(t, b)) {
      String g = conv(r.stores(), ConversationKind.CONVERSATION_KIND_GROUP, "a", "b", "c");
      b.onCall = call -> assertThat(r.stores().own("b", g).readSeq()).isEqualTo(4);
      r.loop().start();
      send(t, read(g, "b", 4));
      await().atMost(Duration.ofSeconds(20)).until(() -> !b.calls.isEmpty());
      assertThat(b.calls).containsExactly("raise b " + g + " 0 4");
    }
  }

  @Test
  void channelReadRaisesNothing() throws Exception {
    String t = topic();
    com.pangate.chat.core.RecordingBadges b = new com.pangate.chat.core.RecordingBadges();
    try (Rig r = rig(t, b)) {
      String ch = conv(r.stores(), ConversationKind.CONVERSATION_KIND_CHANNEL, "o", "f");
      r.loop().start();
      send(t, read(ch, "f", 3));
      await().atMost(Duration.ofSeconds(20)).until(() -> r.stores().own("f", ch).readSeq() == 3);
      Thread.sleep(500);
      assertThat(b.calls).isEmpty();
    }
  }
}
