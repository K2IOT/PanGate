package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakePresence;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.NotifySink;
import com.pangate.chat.core.PrivacyCache;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.NotifyRequest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

/** CH4 spec §4.2: a channel post reaches its offline followers once, paced, resumable. */
class ChannelNotifierIT {

  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  static final int FOLLOWERS = 2_500;

  @BeforeAll
  static void start() {
    kafka.start();
  }

  @AfterAll
  static void stop() {
    kafka.stop();
  }

  final FakeStores stores = new FakeStores();
  final FakePresence presence = new FakePresence();
  final List<NotifyRequest> sent = new CopyOnWriteArrayList<>();
  final NotifySink sink = r -> {
    sent.add(r);
    return CompletableFuture.completedFuture(null);
  };
  final String ch = Ids.next();

  ChannelNotifierIT() {
    List<Member> ms = new ArrayList<>();
    ms.add(new Member("own", Role.OWNER));
    for (int i = 0; i < FOLLOWERS; i++) {
      ms.add(new Member(String.format("f%04d", i), Role.MEMBER));
    }
    stores.create(new Conversation(ch, ConversationKind.CONVERSATION_KIND_CHANNEL, "Tin tức", "own", 0, true,
        ms.size()), ms);
    presence.add("f0001", "n1", 1, "c1");
    presence.add("f0002", "n1", 1, "c2");
    presence.add("f0003", "n1", 1, "c3");
    // review 2026-11-19 (F6): online and with the channel open = received; f0003 is online without it open
    presence.channelOpened("f0001", ch);
    presence.channelOpened("f0002", ch);
    stores.setMute(ch, "f0010", Long.MAX_VALUE);
    stores.setMute(ch, "f0011", Long.MAX_VALUE);
  }

  static String topic() throws Exception {
    String t = "events-" + UUID.randomUUID();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(t, 2, (short) 1))).all().get();
    }
    return t;
  }

  static ChatMessage post(String conv, long seq, long sentAt, String... mentions) {
    return ChatMessage.newBuilder().setConversationId(conv).setMsgId("M" + seq + "-" + UUID.randomUUID())
        .setConvSeq(seq).setSenderId("own").setText("bài " + seq).setSentAtMs(sentAt)
        .addAllMentionUserIds(List.of(mentions)).build();
  }

  static void produce(String topic, ChatMessage m, ConversationKind kind) throws Exception {
    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
      p.send(new ProducerRecord<>(topic, m.getConversationId(), ChatEvent.newBuilder().setMessage(m).setKind(kind)
          .build().toByteArray())).get();
    }
  }

  ChannelNotifierLoop loop(String topic, String group, int rate) {
    return new ChannelNotifierLoop(ChannelNotifierLoop.consumer(kafka.getBootstrapServers(), group), topic, stores,
        presence, stores, stores, new PrivacyCache(stores, () -> 0L), stores, sink, 1_000, rate, Duration.ofHours(1),
        new SimpleMeterRegistry(), System::currentTimeMillis);
  }

  Map<String, Integer> perUser() {
    Map<String, Integer> n = new HashMap<>();
    sent.forEach(r -> n.merge(r.getPrincipalId(), 1, Integer::sum));
    return n;
  }

  @Test
  void offlineUnmutedFollowersGetThePostOnce() throws Exception {
    String t = topic();
    try (ChannelNotifierLoop l = loop(t, "g-" + UUID.randomUUID(), 50_000)) {
      l.start();
      produce(t, post(ch, 1, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      produce(t, post(Ids.next(), 1, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_GROUP);
      await().atMost(Duration.ofSeconds(30)).until(() -> sent.size() >= FOLLOWERS - 4);
      Thread.sleep(1_000);
      assertThat(sent).hasSize(FOLLOWERS - 4);
      assertThat(perUser()).doesNotContainKeys("chat:own", "chat:f0001", "chat:f0002", "chat:f0010")
          .containsKey("chat:f0003").allSatisfy((u, c) -> assertThat(c).isEqualTo(1));
      NotifyRequest r = sent.get(0);
      assertThat(r.getBadge()).isEqualTo(-1);
      assertThat(r.getCollapseId()).isEqualTo(ch);
      assertThat(r.getAlert().getTitle()).isEqualTo("Tin tức");
    }
  }

  @Test
  void aNewPostTakesOverTheRunningJob() throws Exception {
    String t = topic();
    try (ChannelNotifierLoop l = loop(t, "g-" + UUID.randomUUID(), 1_000)) {
      l.start();
      produce(t, post(ch, 1, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      await().atMost(Duration.ofSeconds(10)).until(() -> sent.size() >= 300);
      produce(t, post(ch, 2, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      await().atMost(Duration.ofSeconds(30)).until(() -> sent.size() >= FOLLOWERS - 4);
      Thread.sleep(2_000);
      assertThat(perUser().values()).allSatisfy(c -> assertThat(c).isEqualTo(1));
      assertThat(sent).hasSize(FOLLOWERS - 4);
      assertThat(sent.getLast().getAlert().getBody()).isEqualTo("bài 2");
    }
  }

  /** Review Focus 5. */
  @Test
  void resumesFromProgressAfterRestart() throws Exception {
    String t = topic();
    String group = "g-" + UUID.randomUUID();
    try (ChannelNotifierLoop l = loop(t, group, 1_000)) {
      l.start();
      produce(t, post(ch, 1, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      await().atMost(Duration.ofSeconds(10)).until(() -> stores.loadChannelProgress(ch)
          .filter(p -> !p.lastUserId().isEmpty()).isPresent());
    }
    int before = sent.size();
    try (ChannelNotifierLoop l = loop(t, group, 50_000)) {
      l.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> perUser().size() >= FOLLOWERS - 4);
    }
    assertThat(perUser()).hasSize(FOLLOWERS - 4);
    assertThat(sent.size() - perUser().size()).as("sent again after the restart").isLessThanOrEqualTo(1_000);
    assertThat(before).isPositive();
  }

  @Test
  void aPostOlderThanAnHourIsNotPushed() throws Exception {
    String t = topic();
    try (ChannelNotifierLoop l = loop(t, "g-" + UUID.randomUUID(), 50_000)) {
      l.start();
      produce(t, post(ch, 1, System.currentTimeMillis() - 3_600_001), ConversationKind.CONVERSATION_KIND_CHANNEL);
      Thread.sleep(3_000);
      assertThat(sent).isEmpty();
    }
  }

  @Test
  void pacedToTheRate() throws Exception {
    String t = topic();
    try (ChannelNotifierLoop l = loop(t, "g-" + UUID.randomUUID(), 1_000)) {
      l.start();
      long t0 = System.nanoTime();
      produce(t, post(ch, 1, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      await().atMost(Duration.ofSeconds(30)).until(() -> sent.size() >= FOLLOWERS - 4);
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isGreaterThan(Duration.ofSeconds(2));
    }
  }

  /** Final review: a store failure while a post is taken must not commit past it. */
  @Test
  void aStoreOutageWhileTakingAPostLosesNothing() throws Exception {
    String t = topic();
    try (ChannelNotifierLoop l = loop(t, "g-" + UUID.randomUUID(), 50_000)) {
      stores.fail = true;
      l.start();
      produce(t, post(ch, 1, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      Thread.sleep(3_000);
      stores.fail = false;
      await().atMost(Duration.ofSeconds(30)).until(() -> sent.size() >= FOLLOWERS - 4);
    }
  }

  /** CH4 deferred minor: an unfinished cursor of a post now too old must not be inherited by a later post. */
  @Test
  void aStaleUnfinishedCursorIsNotInherited() throws Exception {
    String t = topic();
    stores.saveChannelProgress(ch, 1, "f1500");   // post 1 stopped half way, then the notifier was down > 1 h
    try (ChannelNotifierLoop l = loop(t, "g-" + UUID.randomUUID(), 50_000)) {
      l.start();
      produce(t, post(ch, 1, System.currentTimeMillis() - 3_600_001), ConversationKind.CONVERSATION_KIND_CHANNEL);
      produce(t, post(ch, 2, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      await().atMost(Duration.ofSeconds(30)).until(() -> sent.size() >= FOLLOWERS - 4);
      Thread.sleep(1_000);
      assertThat(perUser()).containsKey("chat:f0000").hasSize(FOLLOWERS - 4);
    }
  }

  /** CH4 deferred minor: a mention in a post that takes over reaches a muted follower the cursor passed. */
  @Test
  void aMentionInATakeoverPostReachesAFollowerAlreadyPassed() throws Exception {
    String t = topic();
    try (ChannelNotifierLoop l = loop(t, "g-" + UUID.randomUUID(), 1_000)) {
      l.start();
      produce(t, post(ch, 1, System.currentTimeMillis()), ConversationKind.CONVERSATION_KIND_CHANNEL);
      await().atMost(Duration.ofSeconds(10)).until(() -> stores.loadChannelProgress(ch)
          .filter(p -> !p.lastUserId().isEmpty() && !p.done()).isPresent());
      produce(t, post(ch, 2, System.currentTimeMillis(), "f0010", "outsider"),
          ConversationKind.CONVERSATION_KIND_CHANNEL);
      await().atMost(Duration.ofSeconds(30)).until(() -> perUser().containsKey("chat:f0010"));
      Thread.sleep(1_000);
      assertThat(sent).filteredOn(r -> r.getPrincipalId().equals("chat:f0010"))
          .extracting(r -> r.getAlert().getBody()).containsExactly("bài 2");
      assertThat(perUser()).doesNotContainKey("chat:outsider");
    }
  }
}
