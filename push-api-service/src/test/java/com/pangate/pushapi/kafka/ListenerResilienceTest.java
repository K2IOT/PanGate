package com.pangate.pushapi.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.Revoke;
import com.pangate.pushapi.core.ControlState;
import com.pangate.pushapi.core.MessageStatus;
import com.pangate.pushapi.core.TenantPolicy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** Final review: ordinary Kafka errors (a rebalance during commit, a broker outage) stopped the listener threads. */
class ListenerResilienceTest {

  static final Duration WAIT = Duration.ofSeconds(10);

  final Set<String> offline = ConcurrentHashMap.newKeySet();
  final MessageStatus status = new MessageStatus() {
    @Override public void accepted(PushRecord r) { }
    @Override public Optional<Map<String, String>> get(String tenant, String msgId) { return Optional.empty(); }
    @Override public void offline(OfflineRecord o) { offline.add(o.getRecord().getMsgId()); }
    @Override public void delivered(DeliveredRecord d) { }
  };

  @Test
  void aFailedCommitDoesNotStopTheStatusListener() {
    MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      int commits;

      @Override
      public synchronized void commitSync() {
        if (++commits == 1) {
          throw new CommitFailedException();
        }
        super.commitSync();
      }
    };
    TopicPartition tp = new TopicPartition("c1.push.offline", 0);
    StatusListener l = new StatusListener(c, "c1", status, (t, r) -> { });
    l.start();
    try {
      await().atMost(WAIT).until(() -> !c.subscription().isEmpty());
      c.updateBeginningOffsets(Map.of(tp, 0L));
      c.rebalance(List.of(tp));
      c.addRecord(new ConsumerRecord<>(tp.topic(), 0, 0, "k", offline("M1")));
      await().atMost(WAIT).until(() -> offline.contains("M1"));
      c.addRecord(new ConsumerRecord<>(tp.topic(), 0, 1, "k", offline("M2")));
      await().atMost(WAIT).until(() -> offline.contains("M2"));
    } finally {
      l.close();
    }
  }

  @Test
  void aPollErrorDoesNotStopTheControlListener() {
    MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    TopicPartition tp = new TopicPartition("gw.control", 0);
    c.updatePartitions("gw.control", List.of(new PartitionInfo("gw.control", 0, null, null, null)));
    c.updateBeginningOffsets(Map.of(tp, 0L));
    c.updateEndOffsets(Map.of(tp, 0L));
    c.setPollException(new KafkaException("broker gone"));
    ControlState state = new ControlState("c1", new TenantPolicy(false, true, 1, 1, 1), System::currentTimeMillis);
    ControlListener l = new ControlListener(c, "gw.control", state, () -> { });
    l.start();
    try {
      await().atMost(WAIT).until(() -> !c.assignment().isEmpty());
      c.addRecord(new ConsumerRecord<>(tp.topic(), 0, 0, "revoke:j:J1", ControlEvent.newBuilder()
          .setRevoke(Revoke.newBuilder().setJti("J1")).setIssuedAtMs(System.currentTimeMillis()).build()
          .toByteArray()));
      await().atMost(WAIT).until(() -> state.jtiRevoked("J1"));
    } finally {
      l.close();
    }
  }

  static byte[] offline(String msgId) {
    return OfflineRecord.newBuilder().setRecord(PushRecord.newBuilder().setMsgId(msgId).setTenantId("t1")).build()
        .toByteArray();
  }

  /** Spec §6 pushapi.status.lag: from the outcome's at_ms to the status write. */
  @Test
  void statusLagIsMeasuredFromTheOutcomeTime() {
    MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    TopicPartition tp = new TopicPartition("c1.push.offline", 0);
    java.util.List<Long> lags = new java.util.concurrent.CopyOnWriteArrayList<>();
    StatusListener l = new StatusListener(c, "c1", status, (t, r) -> { }, lags::add);
    l.start();
    try {
      await().atMost(WAIT).until(() -> !c.subscription().isEmpty());
      c.updateBeginningOffsets(Map.of(tp, 0L));
      c.rebalance(List.of(tp));
      long at = System.currentTimeMillis() - 5_000;
      c.addRecord(new ConsumerRecord<>(tp.topic(), 0, 0, "k", OfflineRecord.newBuilder().setRecord(PushRecord
          .newBuilder().setMsgId("L1").setTenantId("t1")).setAtMs(at).build().toByteArray()));
      await().atMost(WAIT).until(() -> !lags.isEmpty());
      assertThat(lags.get(0)).isBetween(5_000L, 15_000L);
    } finally {
      l.close();
    }
  }
}
