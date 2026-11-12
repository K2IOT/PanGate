package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.grpc.PushApiClient;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class FanOutIT {

  static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  static int topicNo;

  /** A Push API that records what it got; while {@code down} it answers UNAVAILABLE. */
  static final class FakePushApi extends PushServiceGrpc.PushServiceImplBase {
    final Map<String, Set<String>> delivered = new ConcurrentHashMap<>();   // principal or topic → chat msg ids
    final AtomicBoolean down = new AtomicBoolean();

    @Override
    public StreamObserver<PushRequest> sendStream(StreamObserver<PushResponse> out) {
      return new StreamObserver<>() {
        @Override
        public void onNext(PushRequest r) {
          PushResponse.Builder b = PushResponse.newBuilder().setRequestId(r.getRequestId());
          if (down.get()) {
            b.setCode(PushResponse.Code.CODE_UNAVAILABLE).setRetryAfterMs(50);
          } else {
            try {
              String to = r.getKind() == PushRequest.Kind.KIND_BROADCAST ? r.getTopic() : r.getPrincipalId();
              delivered.computeIfAbsent(to, k -> ConcurrentHashMap.newKeySet())
                  .add(ChatMessage.parseFrom(r.getPayload()).getMsgId());
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
            b.setOk(true).setCode(PushResponse.Code.CODE_OK).setMsgId(Ids.next());
          }
          synchronized (out) {
            out.onNext(b.build());
          }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {
          out.onCompleted();
        }
      };
    }
  }

  @BeforeAll
  static void boot() {
    kafka.start();
  }

  @AfterAll
  static void stop() {
    kafka.stop();
  }

  static String topic() throws Exception {
    String t = "events-" + (++topicNo);
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(t, 2, (short) 1))).all().get();
    }
    return t;
  }

  static StoredMessage msg(String conv, long seq) {
    return new StoredMessage(conv, seq, Ids.next(), "u1", Ids.next(), "m" + seq, "", List.of(), 1_000 + seq);
  }

  static long committed(String topic, String group) throws Exception {
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      Map<TopicPartition, OffsetAndMetadata> m = a.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
          .get();
      return m.values().stream().filter(java.util.Objects::nonNull).mapToLong(OffsetAndMetadata::offset).sum();
    }
  }

  static long end(String topic) throws Exception {
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      return a.listOffsets(Map.of(new TopicPartition(topic, 0), OffsetSpec.latest(), new TopicPartition(topic, 1),
          OffsetSpec.latest())).all().get().values().stream().mapToLong(i -> i.offset()).sum();
    }
  }

  record Rig(FakeStores stores, FakePushApi api, Server server, PushApiClient client, FanOutLoop loop,
      String group) implements AutoCloseable {
    @Override
    public void close() {
      loop.close();
      client.close();
      server.shutdownNow();
    }
  }

  static Rig rig(String topic) throws Exception {
    FakeStores stores = new FakeStores();
    FakePushApi api = new FakePushApi();
    String name = "push-" + Ids.next();
    Server server = InProcessServerBuilder.forName(name).addService(api).build().start();
    PushApiClient client = new PushApiClient(InProcessChannelBuilder.forName(name).build(), Duration.ofSeconds(2));
    String group = "fanout-" + topic;
    FanOutLoop loop = new FanOutLoop(SequencerLoop.consumer(kafka.getBootstrapServers(), group), topic,
        new MemberLookup(stores, stores), stores, client, new SimpleMeterRegistry());
    return new Rig(stores, api, server, client, loop, group);
  }

  @Test
  void groupMembersInboxSeqIsUpdated() throws Exception {
    String t = topic();
    try (Rig r = rig(t); EventProducer events = EventProducer.create(kafka.getBootstrapServers(), t,
        Duration.ofSeconds(5))) {
      String g = Ids.next();
      r.stores().create(new Conversation(g, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 3),
          List.of(new Member("u1", Role.OWNER), new Member("u2", Role.MEMBER), new Member("u3", Role.MEMBER)));
      String ch = Ids.next();
      r.stores().create(new Conversation(ch, ConversationKind.CONVERSATION_KIND_CHANNEL, "c", "u1", 0, true, 1),
          List.of(new Member("u1", Role.OWNER)));
      r.loop().start();
      StoredMessage m1 = msg(g, 1);
      StoredMessage m2 = msg(g, 2);
      StoredMessage c1 = msg(ch, 1);
      events.send(SequencerLoop.event(m1, ConversationKind.CONVERSATION_KIND_GROUP));
      events.send(SequencerLoop.event(m2, ConversationKind.CONVERSATION_KIND_GROUP));
      events.send(SequencerLoop.event(c1, ConversationKind.CONVERSATION_KIND_CHANNEL));
      await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
        for (String u : List.of("u1", "u2", "u3")) {
          assertThat(r.api().delivered.get("chat:" + u)).containsExactlyInAnyOrder(m1.msgId(), m2.msgId());
          assertThat(r.stores().userConvs.get(u).get(g).lastSeq()).isEqualTo(2);
          assertThat(r.stores().userConvs.get(u).get(g).lastMsgAtMs()).isEqualTo(m2.sentAtMs());
        }
        assertThat(r.api().delivered.get("chat:chan:" + ch)).containsExactly(c1.msgId());
      });
      // a channel does not move its followers' inbox rows (spec §5)
      assertThat(r.stores().userConvs.get("u1").get(ch).lastSeq()).isZero();
      await().atMost(Duration.ofSeconds(10)).until(() -> committed(t, r.group()) == end(t));
    }
  }

  @Test
  void unavailablePushApiIsRetriedWithoutLosingEvents() throws Exception {
    String t = topic();
    try (Rig r = rig(t); EventProducer events = EventProducer.create(kafka.getBootstrapServers(), t,
        Duration.ofSeconds(5))) {
      String g = Ids.next();
      r.stores().create(new Conversation(g, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 2),
          List.of(new Member("u1", Role.OWNER), new Member("u2", Role.MEMBER)));
      r.api().down.set(true);
      r.loop().start();
      List<StoredMessage> sent = new java.util.ArrayList<>();
      for (int i = 1; i <= 20; i++) {
        StoredMessage m = msg(g, i);
        sent.add(m);
        events.send(SequencerLoop.event(m, ConversationKind.CONVERSATION_KIND_GROUP));
      }
      Thread.sleep(2_000);
      assertThat(r.api().delivered).isEmpty();
      assertThat(committed(t, r.group())).as("nothing committed while the Push API is down").isZero();
      r.api().down.set(false);
      await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(r.api().delivered.get("chat:u2"))
          .containsExactlyInAnyOrderElementsOf(sent.stream().map(StoredMessage::msgId).toList()));
      await().atMost(Duration.ofSeconds(10)).until(() -> committed(t, r.group()) == end(t));
    }
  }

  @Test
  void streamDroppedIsReopened() throws Exception {
    String t = topic();
    FakeStores stores = new FakeStores();
    FakePushApi api = new FakePushApi();
    String name = "push-" + Ids.next();
    Server first = InProcessServerBuilder.forName(name).addService(api).build().start();
    try (PushApiClient client = new PushApiClient(InProcessChannelBuilder.forName(name).build(),
        Duration.ofSeconds(1));
        FanOutLoop loop = new FanOutLoop(SequencerLoop.consumer(kafka.getBootstrapServers(), "fo-" + t), t,
            new MemberLookup(stores, stores), stores, client, new SimpleMeterRegistry());
        EventProducer events = EventProducer.create(kafka.getBootstrapServers(), t, Duration.ofSeconds(5))) {
      String g = Ids.next();
      stores.create(new Conversation(g, ConversationKind.CONVERSATION_KIND_DIRECT, "", "u1", 0, false, 2),
          List.of(new Member("u1", Role.MEMBER), new Member("u2", Role.MEMBER)));
      loop.start();
      StoredMessage a = msg(g, 1);
      events.send(SequencerLoop.event(a, ConversationKind.CONVERSATION_KIND_DIRECT));
      await().atMost(Duration.ofSeconds(20)).until(() -> api.delivered.containsKey("chat:u2"));
      first.shutdownNow().awaitTermination();   // the Push API restarts
      StoredMessage b = msg(g, 2);
      events.send(SequencerLoop.event(b, ConversationKind.CONVERSATION_KIND_DIRECT));
      Thread.sleep(1_500);
      Server second = InProcessServerBuilder.forName(name).addService(api).build().start();
      try {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(api.delivered.get("chat:u2"))
            .contains(a.msgId(), b.msgId()));
      } finally {
        second.shutdownNow();
      }
    }
  }
}
