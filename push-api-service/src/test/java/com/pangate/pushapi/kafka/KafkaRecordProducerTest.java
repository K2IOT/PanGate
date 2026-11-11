package com.pangate.pushapi.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.pushapi.core.ProduceFailedException;
import java.time.Duration;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/** Final review minor: blocking in send() (metadata) and waiting for the ack shared no deadline (up to 2× timeout). */
class KafkaRecordProducerTest {

  @Test
  void blockingSendAndAckWaitShareOneTimeout() {
    MockProducer<String, byte[]> slow = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer()) {
      @Override
      public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> r) {
        try {
          Thread.sleep(400);      // waiting for metadata
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return super.send(r);    // never acked
      }
    };
    KafkaRecordProducer p = new KafkaRecordProducer(slow, "c1", Duration.ofMillis(500));
    PushRecord r = Fixtures.message("t1:u1");
    long t0 = System.nanoTime();
    assertThatThrownBy(() -> p.send(r)).isInstanceOf(ProduceFailedException.class);
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofMillis(700));
  }
}
