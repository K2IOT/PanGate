package com.pangate.notify.infrastructure.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.notify.application.FakeTokenStore;
import com.pangate.notify.application.TokenRevoker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Review of CH4, Important 1: a coordinator outage makes commitSync time out and then committed() time out too
 * while rewinding. The loop must keep running, not end its thread with the pod still ready.
 */
class LoopSurvivesKafkaOutageTest {

  static final String TOPIC = "t";
  static final TopicPartition TP = new TopicPartition(TOPIC, 0);
  static final byte[] GARBAGE = {(byte) 0xFF};

  /** commitSync and committed() each time out once, as during a coordinator outage. */
  static final class FlakyConsumer extends MockConsumer<String, byte[]> {
    final AtomicInteger commitFailures = new AtomicInteger(1);
    final AtomicInteger committedFailures = new AtomicInteger(1);

    FlakyConsumer() {
      super(OffsetResetStrategy.EARLIEST);
      schedulePollTask(() -> {
        rebalance(List.of(TP));
        updateBeginningOffsets(Map.of(TP, 0L));
        addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "k", GARBAGE));
      });
    }

    @Override
    public synchronized void commitSync() {
      if (commitFailures.getAndDecrement() > 0) {
        throw new TimeoutException("coordinator gone");
      }
      super.commitSync();
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> parts) {
      if (committedFailures.getAndDecrement() > 0) {
        throw new TimeoutException("coordinator gone");
      }
      return super.committed(parts);
    }
  }

  static void await(BooleanSupplier ok) throws InterruptedException {
    long end = System.nanoTime() + 10_000_000_000L;
    while (!ok.getAsBoolean()) {
      assertThat(System.nanoTime()).as("timed out").isLessThan(end);
      Thread.sleep(20);
    }
  }

  @Test
  void requestLoopKeepsRunningWhenTheRewindTimesOut() throws Exception {
    FlakyConsumer c = new FlakyConsumer();
    SimpleMeterRegistry meters = new SimpleMeterRegistry();
    NotifyRequestLoop loop = new NotifyRequestLoop(c, TOPIC, null, 10, meters, System::currentTimeMillis);
    loop.start();
    try {
      await(() -> c.committedFailures.get() < 0);
      c.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", GARBAGE));
      await(() -> meters.counter("notify.requests", "result", "invalid").count() >= 2);
      await(() -> c.committed(Set.of(TP)).get(TP) != null);
    } finally {
      loop.close();
    }
  }

  @Test
  void revocationDeleterKeepsRunningWhenTheRewindTimesOut() throws Exception {
    FlakyConsumer c = new FlakyConsumer();
    AtomicInteger malformed = new AtomicInteger();
    RevocationDeleter loop = new RevocationDeleter(c, TOPIC,
        new TokenRevoker(new FakeTokenStore(), new SimpleMeterRegistry()), malformed::incrementAndGet);
    loop.start();
    try {
      await(() -> c.committedFailures.get() < 0);
      c.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", GARBAGE));
      await(() -> malformed.get() >= 2);
      await(() -> c.committed(Set.of(TP)).get(TP) != null);
    } finally {
      loop.close();
    }
  }
}
