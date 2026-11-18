package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.Badge;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.OfflineNotifications;
import com.pangate.chat.core.PrivacyCache;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

/** CH4 spec §3.1: cN.push.offline → cN.notify.requests, at least once, store failures retried. */
class OfflineNotifierIT {

  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

  @BeforeAll
  static void start() {
    kafka.start();
  }

  @AfterAll
  static void stop() {
    kafka.stop();
  }

  static String topic(String name) throws Exception {
    String t = name + "-" + UUID.randomUUID();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(t, 2, (short) 1))).all().get();
    }
    return t;
  }

  final FakeStores stores = new FakeStores();
  final String dm = Ids.next();

  OfflineNotifierIT() {
    stores.create(new Conversation(dm, ConversationKind.CONVERSATION_KIND_DIRECT, "", "a", 0, false, 2),
        List.of(new Member("a", Role.MEMBER), new Member("b", Role.MEMBER)));
    stores.put("a", "active", "An", 1);
    stores.touchUserConv("b", dm, 3, 1);
  }

  static OfflineRecord offline(String conv, String to) {
    ChatMessage m = ChatMessage.newBuilder().setConversationId(conv).setMsgId("M-" + UUID.randomUUID()).setConvSeq(3)
        .setSenderId("a").setText("chào").setSentAtMs(System.currentTimeMillis()).build();
    return OfflineRecord.newBuilder().setRecord(PushRecord.newBuilder().setTenantId("chat").setPrincipalId("chat:" + to)
        .setFrame(Frame.newBuilder().setPayload(m.toByteString()).putHeaders("type", "chat.message")))
        .setReason(OfflineRecord.Reason.REASON_NO_SESSION).build();
  }

  static void produce(String topic, OfflineRecord r) throws Exception {
    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
      p.send(new ProducerRecord<>(topic, r.getRecord().getPrincipalId(), r.toByteArray())).get();
    }
  }

  static List<ConsumerRecord<String, byte[]>> read(String topic, int atLeast) {
    List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers(), "group.id", "it-" + UUID.randomUUID(), "auto.offset.reset", "earliest"),
        new StringDeserializer(), new ByteArrayDeserializer())) {
      c.subscribe(List.of(topic));
      long end = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (out.size() < atLeast && System.nanoTime() < end) {
        c.poll(Duration.ofMillis(200)).forEach(out::add);
      }
    }
    return out;
  }

  OfflineNotifierLoop loop(String in, String out) {
    OfflineNotifications n = new OfflineNotifications(stores, stores, new PrivacyCache(stores, () -> 0L), stores,
        new Badge(stores), Duration.ofHours(1));
    NotifyProducer sink = new NotifyProducer(NotifyProducer.producer(kafka.getBootstrapServers()), out,
        Duration.ofSeconds(5));
    return new OfflineNotifierLoop(OfflineNotifierLoop.consumer(kafka.getBootstrapServers(), "chat-notify-" + in), in,
        n, sink, new SimpleMeterRegistry());
  }

  @Test
  void offlineDmBecomesANotifyRequest() throws Exception {
    String in = topic("off");
    String out = topic("notify");
    try (OfflineNotifierLoop l = loop(in, out)) {
      l.start();
      produce(in, offline(dm, "b"));
      List<ConsumerRecord<String, byte[]>> got = read(out, 1);
      assertThat(got).hasSize(1);
      assertThat(got.get(0).key()).isEqualTo("chat:b");
      NotifyRequest r = NotifyRequest.parseFrom(got.get(0).value());
      assertThat(r.getAlert().getTitle()).isEqualTo("An");
      assertThat(r.getAlert().getBody()).isEqualTo("chào");
      assertThat(r.getBadge()).isEqualTo(3);
    }
  }

  /** Review Focus 3: the router writing the same OfflineRecord twice gives two requests with one request id. */
  @Test
  void replayedOfflineRecordNotifiesOnce() throws Exception {
    String in = topic("off");
    String out = topic("notify");
    OfflineRecord r = offline(dm, "b");
    try (OfflineNotifierLoop l = loop(in, out)) {
      l.start();
      produce(in, r);
      produce(in, r);
      List<ConsumerRecord<String, byte[]>> got = read(out, 2);
      assertThat(got).hasSize(2);
      assertThat(NotifyRequest.parseFrom(got.get(0).value()).getRequestId())
          .isEqualTo(NotifyRequest.parseFrom(got.get(1).value()).getRequestId());
    }
  }

  @Test
  void storeOutageIsRetried() throws Exception {
    String in = topic("off");
    String out = topic("notify");
    stores.fail = true;
    try (OfflineNotifierLoop l = loop(in, out)) {
      l.start();
      produce(in, offline(dm, "b"));
      Thread.sleep(2_000);
      stores.fail = false;
      await().atMost(Duration.ofSeconds(20)).until(() -> !read(out, 1).isEmpty());
    }
  }
}
