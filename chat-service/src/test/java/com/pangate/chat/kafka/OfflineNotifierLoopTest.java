package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;

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

/** Review of CH4, Important 1: a timed-out commit followed by a timed-out rewind must not end the loop's thread. */
class OfflineNotifierLoopTest {

  static final TopicPartition TP = new TopicPartition("t", 0);
  static final byte[] GARBAGE = {(byte) 0xFF};

  @Test
  void keepsRunningWhenTheRewindTimesOut() throws Exception {
    AtomicInteger commitFailures = new AtomicInteger(1);
    AtomicInteger committedFailures = new AtomicInteger(1);
    MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
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
    };
    c.schedulePollTask(() -> {
      c.rebalance(List.of(TP));
      c.updateBeginningOffsets(Map.of(TP, 0L));
      c.addRecord(new ConsumerRecord<>("t", 0, 0, "k", GARBAGE));
    });
    SimpleMeterRegistry meters = new SimpleMeterRegistry();
    OfflineNotifierLoop loop = new OfflineNotifierLoop(c, "t", null, null, meters);
    loop.start();
    try {
      await(() -> committedFailures.get() < 0);
      c.addRecord(new ConsumerRecord<>("t", 0, 1, "k", GARBAGE));
      await(() -> meters.counter("chat.notify.offline", "result", "invalid").count() >= 2);
      await(() -> c.committed(Set.of(TP)).get(TP) != null);
    } finally {
      loop.close();
    }
  }

  static void await(BooleanSupplier ok) throws InterruptedException {
    long end = System.nanoTime() + 10_000_000_000L;
    while (!ok.getAsBoolean()) {
      assertThat(System.nanoTime()).as("timed out").isLessThan(end);
      Thread.sleep(20);
    }
  }
}
