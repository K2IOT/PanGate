package com.pangate.chat.measure;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakePresence;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.NotifySink;
import com.pangate.chat.core.PrivacyCache;
import com.pangate.chat.kafka.ChannelNotifierLoop;
import com.pangate.chat.kafka.NotifyProducer;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * CH4 spec §0, §7: one post to a channel of 100 000 offline followers, at the default 5 000 requests/s per pod, the
 * requests produced to a real Kafka. Run: {@code -Dit.test=ChannelNotifyMeasureIT -Dchat.notify.measure=true}.
 */
class ChannelNotifyMeasureIT {

  static final int FOLLOWERS = 100_000;

  @Test
  void hundredThousandFollowers() throws Exception {
    Assumptions.assumeTrue(Boolean.getBoolean("chat.notify.measure"), "set -Dchat.notify.measure=true to measure");
    try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))) {
      kafka.start();
      String events = "events-" + UUID.randomUUID();
      String requests = "requests-" + UUID.randomUUID();
      try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
        a.createTopics(List.of(new NewTopic(events, 2, (short) 1), new NewTopic(requests, 8, (short) 1))).all().get();
      }
      FakeStores stores = new FakeStores();
      String ch = Ids.next();
      List<Member> ms = new ArrayList<>(FOLLOWERS + 1);
      ms.add(new Member("owner", Role.OWNER));
      for (int i = 0; i < FOLLOWERS; i++) {
        ms.add(new Member(String.format("f%06d", i), Role.MEMBER));
      }
      stores.create(new Conversation(ch, ConversationKind.CONVERSATION_KIND_CHANNEL, "Big", "owner", 0, true,
          ms.size()), ms);
      AtomicInteger acked = new AtomicInteger();
      NotifyProducer producer = new NotifyProducer(NotifyProducer.producer(kafka.getBootstrapServers()), requests,
          Duration.ofSeconds(10));
      NotifySink sink = r -> producer.send(r).thenRun(acked::incrementAndGet);
      try (ChannelNotifierLoop loop = new ChannelNotifierLoop(ChannelNotifierLoop.consumer(
          kafka.getBootstrapServers(), "measure"), events, stores, new FakePresence(), stores, stores,
          new PrivacyCache(stores, () -> 0L), stores, sink, 1_000, 5_000, Duration.ofHours(1),
          new SimpleMeterRegistry(), System::currentTimeMillis); producer) {
        loop.start();
        ChatMessage post = ChatMessage.newBuilder().setConversationId(ch).setMsgId("M1").setConvSeq(1)
            .setSenderId("owner").setText("big news").setSentAtMs(System.currentTimeMillis()).build();
        long t0 = System.nanoTime();
        try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers",
            kafka.getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
          p.send(new ProducerRecord<>(events, ch, ChatEvent.newBuilder().setMessage(post)
              .setKind(ConversationKind.CONVERSATION_KIND_CHANNEL).build().toByteArray())).get();
        }
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        while (acked.get() < FOLLOWERS && System.nanoTime() < deadline) {
          Thread.sleep(100);
        }
        double s = (System.nanoTime() - t0) / 1e9;
        System.out.printf("CHANNEL NOTIFY MEASURE: %d followers -> %d requests acked in %.1f s (%.0f/s)%n",
            FOLLOWERS, acked.get(), s, acked.get() / s);
        assertThat(acked.get()).isEqualTo(FOLLOWERS);
      }
    }
  }
}
