package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.FakePresence;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.PresenceTracker;
import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Review 2026-11-19 (F5): a lifecycle event whose store write failed is read again, not skipped, so a lost
 * disconnect cannot leave a ghost connection that keeps a user online.
 */
class LifecycleListenerTest {

  static final TopicPartition TP = new TopicPartition("c1.gw.lifecycle", 0);

  static ConsumerRecord<String, byte[]> rec(long offset, EventType t, String id) {
    ConnectionEvent e = ConnectionEvent.newBuilder().setType(t).setPrincipalId("chat:a").setNodeId("n1").setEpoch(1)
        .setConnId("c1").setTenantId("chat").setEventId(id).build();
    return new ConsumerRecord<>(TP.topic(), 0, offset, "chat:a", e.toByteArray());
  }

  @Test
  void aDisconnectThatFailedIsReadAgain() throws Exception {
    MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    c.schedulePollTask(() -> {
      c.rebalance(List.of(TP));
      c.updateBeginningOffsets(Map.of(TP, 0L));
      c.addRecord(rec(0, EventType.EVENT_TYPE_CONNECTED, "e0"));
      c.addRecord(rec(1, EventType.EVENT_TYPE_DISCONNECTED, "e1"));
    });
    FakePresence state = new FakePresence();
    state.failRemoves = 1;
    List<PresenceTracker.Transition> out = new CopyOnWriteArrayList<>();
    PresenceTracker tracker = new PresenceTracker(state, new FakeStores(), out::add, System::currentTimeMillis,
        Duration.ofMillis(50));
    LifecycleListener l = new LifecycleListener(c, TP.topic(), tracker, new SimpleMeterRegistry());
    l.start();
    try {
      // the failed disconnect is not committed past: the broker hands it out again (MockConsumer: re-added)
      await(() -> committed(c) == 1);
      assertThat(state.online("a")).isTrue();
      c.addRecord(rec(1, EventType.EVENT_TYPE_DISCONNECTED, "e1"));
      await(() -> !state.online("a") && out.stream().anyMatch(t -> !t.online()));
      await(() -> committed(c) == 2);
    } finally {
      l.close();
    }
  }

  static long committed(MockConsumer<String, byte[]> c) {
    var m = c.committed(Set.of(TP)).get(TP);
    return m == null ? -1 : m.offset();
  }

  static void await(BooleanSupplier ok) throws InterruptedException {
    long end = System.nanoTime() + 10_000_000_000L;
    while (!ok.getAsBoolean()) {
      assertThat(System.nanoTime()).as("timed out").isLessThan(end);
      Thread.sleep(20);
    }
  }
}
