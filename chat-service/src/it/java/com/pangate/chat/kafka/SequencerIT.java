package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.SendResults;
import com.pangate.chat.core.SequencedMessage;
import com.pangate.chat.core.Sequencer;
import com.pangate.chat.store.RedisChatCache;
import com.pangate.chat.store.SchemaMigrator;
import com.pangate.chat.store.ScyllaChatStore;
import com.pangate.chat.store.ScyllaSession;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

class SequencerIT {

  static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  static final ScyllaDBContainer scylla = new ScyllaDBContainer("scylladb/scylla:6.2.3");
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static CqlSession session;
  static ScyllaChatStore store;
  static RedisChatCache cache;
  static int topicNo;

  @BeforeAll
  static void boot() {
    kafka.start();
    scylla.start();
    redis.start();
    session = ScyllaSession.connect(new ChatProperties.Scylla(
        List.of(scylla.getHost() + ":" + scylla.getMappedPort(9042)), "datacenter1", "chat", Duration.ofSeconds(5), 1));
    SchemaMigrator.migrate(session, "chat");
    store = new ScyllaChatStore(session, "chat");
    cache = new RedisChatCache(new ChatProperties.Redis("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
        Duration.ofSeconds(1), Duration.ofSeconds(60)));
  }

  @AfterAll
  static void shutdown() {
    cache.close();
    session.close();
    redis.stop();
    scylla.stop();
    kafka.stop();
  }

  /** Fresh command and event topics per test: each test reads its events from the start. */
  static String[] topics() throws Exception {
    topicNo++;
    String cmds = "cmds-" + topicNo;
    String events = "events-" + topicNo;
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic(cmds, 4, (short) 1), new NewTopic(events, 4, (short) 1),
          new NewTopic(ChatTopics.DLQ + "-" + topicNo, 1, (short) 1))).all().get();
    }
    return new String[] {cmds, events};
  }

  static String conv() {
    String id = Ids.next();
    store.create(new Conversation(id, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
    return id;
  }

  static ChatCommand cmd(String conv, String clientMsgId) {
    return ChatCommand.newBuilder().setCommandId(Ids.next()).setSenderId("u1").setSenderDeviceId("d1")
        .setReceivedAtMs(System.currentTimeMillis()).setSend(ChatSend.newBuilder().setConversationId(conv)
            .setClientMsgId(clientMsgId).setText("m " + clientMsgId)).build();
  }

  SequencerLoop loop(String cmds, String events) {
    Sequencer s = new Sequencer(store, store, store, cache, System::currentTimeMillis);
    return new SequencerLoop(SequencerLoop.consumer(kafka.getBootstrapServers(), "seq-" + cmds), cmds, s,
        EventProducer.create(kafka.getBootstrapServers(), events, Duration.ofSeconds(5)), SendResults.NONE,
        CommandProducer.create(kafka.getBootstrapServers(), Duration.ofSeconds(5)), new SimpleMeterRegistry());
  }

  static List<ChatEvent> readEvents(String topic, int atLeast, Duration within) throws Exception {
    List<ChatEvent> out = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of("bootstrap.servers",
        kafka.getBootstrapServers(), "group.id", "read-" + Ids.next(), "auto.offset.reset", "earliest"),
        new StringDeserializer(), new ByteArrayDeserializer())) {
      c.subscribe(List.of(topic));
      long deadline = System.currentTimeMillis() + within.toMillis();
      while (out.size() < atLeast && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> r : c.poll(Duration.ofMillis(200))) {
          ChatEvent e = ChatEvent.parseFrom(r.value());
          assertThat(r.key()).isEqualTo(e.getMessage().getConversationId());
          out.add(e);
        }
      }
      for (ConsumerRecord<String, byte[]> r : c.poll(Duration.ofSeconds(1))) {
        out.add(ChatEvent.parseFrom(r.value()));
      }
    }
    return out;
  }

  @Test
  void twoSequencersNeverReuseASeq() throws Exception {
    String[] t = topics();
    List<String> convs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      convs.add(conv());
    }
    Map<String, Set<String>> keys = new HashMap<>();
    try (CommandProducer p = CommandProducer.create(kafka.getBootstrapServers(), Duration.ofSeconds(5), t[0]);
        SequencerLoop a = loop(t[0], t[1]); SequencerLoop b = loop(t[0], t[1])) {
      a.start();
      b.start();
      for (int i = 0; i < 1_000; i++) {
        String conv = convs.get(i % 10);
        // every 7th send is a retry of an earlier one
        String k = i % 7 == 6 && !keys.getOrDefault(conv, Set.of()).isEmpty()
            ? keys.get(conv).iterator().next() : Ids.next();
        keys.computeIfAbsent(conv, x -> new HashSet<>()).add(k);
        p.publish(cmd(conv, k));
        if (i == 400) {
          a.close();   // rebalance mid-way: b takes a's partitions, maybe with uncommitted records
        }
      }
      int distinct = keys.values().stream().mapToInt(Set::size).sum();
      List<ChatEvent> events = readEvents(t[1], 1_000, Duration.ofSeconds(60));
      Map<String, Map<Long, String>> seqToMsg = new HashMap<>();
      for (ChatEvent e : events) {
        String prev = seqToMsg.computeIfAbsent(e.getMessage().getConversationId(), x -> new HashMap<>())
            .putIfAbsent(e.getMessage().getConvSeq(), e.getMessage().getMsgId());
        assertThat(prev == null || prev.equals(e.getMessage().getMsgId())).as("one message per seq").isTrue();
      }
      for (String conv : convs) {
        Map<Long, String> m = seqToMsg.get(conv);
        int n = keys.get(conv).size();
        assertThat(m.keySet()).as("seqs of " + conv).containsExactlyInAnyOrderElementsOf(
            java.util.stream.LongStream.rangeClosed(1, n).boxed().toList());
        assertThat(store.lastSeq(conv)).isEqualTo(n);
        assertThat(new HashSet<>(m.values())).hasSize(n);
      }
      assertThat(seqToMsg.values().stream().mapToInt(Map::size).sum()).isEqualTo(distinct);
    }
  }

  @Test
  void crashAfterWriteBeforeCommitKeepsOneMessage() throws Exception {
    String[] t = topics();
    String conv = conv();
    String k = Ids.next();
    ChatCommand c = cmd(conv, k);
    try (CommandProducer p = CommandProducer.create(kafka.getBootstrapServers(), Duration.ofSeconds(5), t[0]);
        EventProducer events = EventProducer.create(kafka.getBootstrapServers(), t[1], Duration.ofSeconds(5))) {
      p.publish(c);
      // a sequencer that wrote the message and its event, then died before committing the offset
      Sequencer crashed = new Sequencer(store, store, store, cache, System::currentTimeMillis);
      SequencedMessage m = crashed.process(conv, List.of(c)).getFirst();
      events.send(SequencerLoop.event(m.message(), ConversationKind.CONVERSATION_KIND_GROUP));
    }
    try (SequencerLoop again = loop(t[0], t[1])) {
      again.start();
      List<ChatEvent> evs = readEvents(t[1], 2, Duration.ofSeconds(30));
      assertThat(evs).hasSize(2);
      assertThat(evs).extracting(e -> e.getMessage().getMsgId()).containsOnly(evs.getFirst().getMessage().getMsgId());
      assertThat(evs).extracting(e -> e.getMessage().getConvSeq()).containsOnly(1L);
      assertThat(evs.get(1).getMessage().getText()).isEqualTo("m " + k);
      assertThat(store.lastSeq(conv)).isEqualTo(1);
      assertThat(store.after(conv, 0, 10)).extracting(StoredMessage::clientMsgId).containsExactly(k);
    }
  }

}
