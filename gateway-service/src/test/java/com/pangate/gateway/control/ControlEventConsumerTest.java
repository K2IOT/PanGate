package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.CellMap;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.Revoke;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ControlEventConsumerTest {

  private static final String TOPIC = "gw.control";
  private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);

  private final MockConsumer<String, byte[]> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
  private final NodeState node = new NodeState();
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final List<String> seen = new CopyOnWriteArrayList<>();
  private final List<String> exits = new CopyOnWriteArrayList<>();
  private final AtomicLong mono = new AtomicLong();
  private ControlEventConsumer consumer;

  private final ControlEventHandler recorder = new ControlEventHandler() {
    @Override
    public void onRevoke(String key, long expiresAtMs) {
      seen.add("revoke " + key);
    }

    @Override
    public void onCellMap(String tenant, String cell) {
      seen.add("cellmap " + tenant + "=" + cell);
    }

    @Override
    public void onCellMapRemoved(String tenant) {
      seen.add("cellmap- " + tenant);
    }
  };

  private void start(long endOffset, Duration catchUpLimit) {
    Node n = new Node(0, "h", 1);
    mock.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, n, new Node[] {n}, new Node[] {n})));
    mock.updateBeginningOffsets(Map.of(P0, 0L));
    mock.updateEndOffsets(Map.of(P0, endOffset));
    consumer = new ControlEventConsumer(() -> mock, TOPIC, node, List.of(recorder), new GatewayMetrics(registry),
        (status, reason) -> exits.add(status + ":" + reason), catchUpLimit, mono::get);
    consumer.start();
  }

  private void add(long offset, String key, byte[] value) {
    mock.schedulePollTask(() -> mock.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, key, value)));
  }

  private double errors() {
    var c = registry.find("gw.control.consumer.errors").tag("reason", "bad_record").counter();
    return c == null ? 0 : c.count();
  }

  @AfterEach
  void stop() {
    if (consumer != null) {
      consumer.stop();
    }
  }

  @Test
  void caughtUpImmediatelyOnEmptyTopic() {
    start(0, Duration.ofMinutes(5));

    await().atMost(Duration.ofSeconds(5)).until(node::controlCaughtUp);
    assertThat(consumer.caughtUp()).isTrue();
  }

  @Test
  void malformedAndUnknownRecordsAreSkippedAndCatchUpCompletes() {
    add(0, "revoke:p:t1:u1", new byte[] {(byte) 0xff, 0x01, 0x02});
    add(1, "zzz", ControlEvent.newBuilder().setCellMap(CellMap.newBuilder().setTenantId("t").setCell("c")).build()
        .toByteArray());
    add(2, "revoke:p:t1:u1", ControlEvent.getDefaultInstance().toByteArray());
    add(3, "zzz", null);
    add(4, "revoke:p:t1:u9", ControlEvent.newBuilder().setRevoke(Revoke.newBuilder().setPrincipalId("t1:u9")
        .setExpiresAtMs(Long.MAX_VALUE)).build().toByteArray());
    start(5, Duration.ofMinutes(5));

    await().atMost(Duration.ofSeconds(5)).until(node::controlCaughtUp);
    assertThat(errors()).isEqualTo(3);
    assertThat(seen).containsExactly("cellmap t=c", "revoke p:t1:u9");
  }

  @Test
  void tombstoneDispatchesRemoval() {
    add(0, "cellmap:t2", null);
    start(1, Duration.ofMinutes(5));

    await().atMost(Duration.ofSeconds(5)).until(node::controlCaughtUp);
    assertThat(seen).containsExactly("cellmap- t2");
  }

  @Test
  void neverCaughtUpExitsAfterLimit() {
    start(10, Duration.ofSeconds(2));
    mono.set(3_000);

    await().atMost(Duration.ofSeconds(5)).until(() -> !exits.isEmpty());
    assertThat(exits.get(0)).startsWith("1:").contains("not caught up");
    assertThat(node.controlCaughtUp()).isFalse();
  }
  @Test
  void consumerThatCannotBeCreatedExitsInsteadOfStayingUnready() {
    consumer = new ControlEventConsumer(() -> {
      throw new org.apache.kafka.common.KafkaException("bad config");
    }, TOPIC, node, List.of(recorder), new GatewayMetrics(registry), (status, reason) -> exits.add(status + ":"
        + reason), Duration.ofMinutes(5), mono::get);

    consumer.start();

    await().atMost(Duration.ofSeconds(5)).until(() -> !exits.isEmpty());
    assertThat(exits.get(0)).startsWith("1:").contains("bad config");
  }
}
