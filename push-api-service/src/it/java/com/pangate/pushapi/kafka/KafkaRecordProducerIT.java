package com.pangate.pushapi.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.PushPathScenario;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.pushapi.core.ProduceFailedException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaRecordProducerIT {

  @RegisterExtension static PushPathScenario scenario = new PushPathScenario("c1");

  @Test
  void recordsFollowKeyAndHeaderConventions() {
    try (KafkaRecordProducer p = KafkaRecordProducer.create(scenario.bootstrapServers(), "c1", Duration.ofSeconds(5))) {
      PushRecord r = Fixtures.withMsgId(Fixtures.message("t1:u1"), "01JPRODUCERIT0000000000000");
      p.send(r);
      assertThat(scenario.awaitRequest(r.getMsgId())).isEqualTo(r);
      PushRecord b = Fixtures.withMsgId(Fixtures.broadcast("t1:news"), "01JPRODUCERIT000000000000B");
      p.send(b);
      assertThat(scenario.awaitRequest(b.getMsgId())).isEqualTo(b);
    }
  }

  @Test
  void brokerDownIsProduceFailedWithinTimeout() {
    try (KafkaRecordProducer p = KafkaRecordProducer.create("127.0.0.1:1", "c1", Duration.ofMillis(500))) {
      long t0 = System.nanoTime();
      assertThatThrownBy(() -> p.send(Fixtures.message("t1:u1"))).isInstanceOf(ProduceFailedException.class);
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(3));
    }
  }
}
