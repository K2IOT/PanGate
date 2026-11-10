package com.pangate.gateway.upstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.gateway.lifecycle.TestKafka;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/** Lifecycle events carry a ULID event_id (push contracts spec §1.2, §5). */
class LifecycleEventIdTest {

  private final LoopFixture f = new LoopFixture();
  private final MockProducer<String, byte[]> reliable =
      new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
  private final LifecyclePublisher lifecycle = new LifecyclePublisher(TestKafka.of(reliable, f.telemetry), f.topics,
      f.node, "cell-1", f.metrics, () -> 5_000L);

  @Test
  void everyEventCarriesADistinctEventId() throws Exception {
    Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "2.1", Set.of()), "CONN", 1L, f.wallNow(),
        f.wallNow() + 3_600, 0, List.of());
    lifecycle.activated(s);
    s.close(CloseCode.REVOKED, "REVOKED");
    lifecycle.closed(s);

    String a = ConnectionEvent.parseFrom(reliable.history().get(0).value()).getEventId();
    String b = ConnectionEvent.parseFrom(reliable.history().get(1).value()).getEventId();
    assertThat(a).matches(EventIdsTest.ULID);
    assertThat(b).matches(EventIdsTest.ULID).isNotEqualTo(a);
  }
}
