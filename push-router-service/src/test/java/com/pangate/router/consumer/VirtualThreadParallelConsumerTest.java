package com.pangate.router.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class VirtualThreadParallelConsumerTest {

  @Test
  void workersAreVirtualThreads() {
    TopicPartition tp = new TopicPartition("c1.push.requests", 0);
    MockConsumer<String, byte[]> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    var options = ParallelConsumerOptions.<String, byte[]>builder().consumer(mock)
        .ordering(ParallelConsumerOptions.ProcessingOrder.KEY).maxConcurrency(4).build();
    List<Boolean> virtual = new CopyOnWriteArrayList<>();
    try (var pc = new VirtualThreadParallelConsumer<>(options)) {
      pc.subscribe(List.of(tp.topic()));
      mock.rebalance(List.of(tp));
      mock.updateBeginningOffsets(Map.of(tp, 0L));
      mock.seek(tp, 0);
      for (int i = 0; i < 3; i++) {
        mock.addRecord(new ConsumerRecord<>(tp.topic(), 0, i, "t1:u" + i, new byte[] {1}));
      }
      pc.poll(ctx -> virtual.add(Thread.currentThread().isVirtual()));
      await().atMost(Duration.ofSeconds(10)).until(() -> virtual.size() == 3);
    }
    assertThat(virtual).containsOnly(true);
  }
}
