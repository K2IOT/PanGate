package com.pangate.router.nodes;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NodeDownWatcherTest {

  final List<String> emitted = new ArrayList<>();
  final NodeDownWatcher watcher = new NodeDownWatcher((id, epoch) -> emitted.add(id + ":" + epoch));

  static Map<String, Node> nodes(Node... n) {
    var m = new java.util.HashMap<String, Node>();
    for (Node x : n) {
      m.put(x.id(), x);
    }
    return m;
  }

  @Test
  void lostLeaseEmitsNodeDownOnce() {
    Node a = new Node("a", 3, "h:1", false);
    watcher.accept(nodes(), nodes(a));
    watcher.accept(nodes(a), nodes());
    watcher.accept(nodes(), nodes());
    watcher.accept(nodes(a), nodes());  // same id:epoch again (e.g. a flapping snapshot)
    assertThat(emitted).containsExactly("a:3");
  }

  @Test
  void epochChangeEmitsNodeDownForTheOldEpoch() {
    watcher.accept(nodes(new Node("a", 3, "h:1", false)), nodes(new Node("a", 4, "h:1", false)));
    assertThat(emitted).containsExactly("a:3");
  }

  @Test
  void nodesNeverSeenEmitNothing() {
    watcher.accept(nodes(), nodes(new Node("b", 1, "h:2", false)));
    assertThat(emitted).isEmpty();
  }

  @Test
  void kafkaPublisherWritesTheContractRecord() throws Exception {
    var mock = new org.apache.kafka.clients.producer.MockProducer<String, byte[]>(true,
        new org.apache.kafka.common.serialization.StringSerializer(),
        new org.apache.kafka.common.serialization.ByteArraySerializer());
    new KafkaLifecyclePublisher(mock, "c1", () -> 1_762_732_800_000L).nodeDown("gw-1", 7);
    var r = mock.history().getFirst();
    assertThat(r.topic()).isEqualTo("c1.gw.lifecycle");
    var e = com.pangate.contracts.v1.ConnectionEvent.parseFrom(r.value());
    assertThat(com.pangate.contracts.testkit.PushKeys.of(e)).isEqualTo(r.key()).isEqualTo("gw-1");
    assertThat(e.getEpoch()).isEqualTo(7);
    assertThat(e.getEventId()).matches("[0-9A-HJKMNP-TV-Z]{26}");
    var cr = new org.apache.kafka.clients.consumer.ConsumerRecord<>(r.topic(), 0, 0, 0L,
        org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, r.key(), r.value(), r.headers(),
        java.util.Optional.empty());
    com.pangate.contracts.testkit.PushHeaders.assertConventions(cr, "ConnectionEvent");
  }
}
