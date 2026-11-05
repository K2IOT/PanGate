package com.pangate.gateway.upstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import com.pangate.gateway.lifecycle.TestKafka;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import com.pangate.starter.kafka.PangateHeaders;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class LifecyclePublisherTest {

  private final LoopFixture f = new LoopFixture();
  private final MockProducer<String, byte[]> reliable =
      new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
  private final LifecyclePublisher lifecycle = new LifecyclePublisher(TestKafka.of(reliable, f.telemetry), f.topics,
      f.node, "cell-1", f.metrics, () -> 5_000L);

  private Session active() {
    Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "2.1", Set.of()), "CONN", 1L, f.wallNow(),
        f.wallNow() + 3_600, 0, List.of());
    return s;
  }

  private ConnectionEvent event(int i) throws Exception {
    return ConnectionEvent.parseFrom(reliable.history().get(i).value());
  }

  @Test
  void connectedAndDisconnectedEventsCarryEpochAndCloseCode() throws Exception {
    Session s = active();

    lifecycle.activated(s);
    s.close(CloseCode.REVOKED, "REVOKED");
    lifecycle.closed(s);

    ConnectionEvent c = event(0);
    assertThat(reliable.history().get(0).topic()).isEqualTo("c1.gw.lifecycle");
    assertThat(reliable.history().get(0).key()).isEqualTo("t1:u1");
    assertThat(PangateHeaders.read(reliable.history().get(0).headers(), "content-type"))
        .hasValue("application/x-protobuf; schema=pangate.v1.ConnectionEvent");
    assertThat(c.getType()).isEqualTo(EventType.EVENT_TYPE_CONNECTED);
    assertThat(c.getConnId()).isEqualTo("CONN");
    assertThat(c.getEpoch()).isEqualTo(7);
    assertThat(c.getNodeId()).isEqualTo("n1");
    assertThat(c.getCellId()).isEqualTo("cell-1");
    assertThat(c.getPlatform()).isEqualTo("ios");
    assertThat(c.getSdkVersion()).isEqualTo("2.1");
    assertThat(c.getAtMs()).isEqualTo(5_000);
    ConnectionEvent d = event(1);
    assertThat(d.getType()).isEqualTo(EventType.EVENT_TYPE_DISCONNECTED);
    assertThat(d.getCloseCode()).isEqualTo(4003);
  }

  @Test
  void heartbeatTimeoutAndKickedMapped() throws Exception {
    Session a = active();
    a.close(CloseCode.GOING_AWAY, "HEARTBEAT_TIMEOUT");
    lifecycle.closed(a);
    LoopFixture g = new LoopFixture();
    Session b = g.tracker.admit(g.channel, g.loop, InetAddress.getLoopbackAddress());
    b.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "2.1", Set.of()), "C2", 1L, g.wallNow(),
        g.wallNow() + 3_600, 0, List.of());
    b.close(CloseCode.DUPLICATE_SESSION, "KICKED");
    lifecycle.closed(b);

    assertThat(event(0).getType()).isEqualTo(EventType.EVENT_TYPE_HEARTBEAT_TIMEOUT);
    assertThat(event(0).getCloseCode()).isEqualTo(1001);
    assertThat(event(1).getType()).isEqualTo(EventType.EVENT_TYPE_KICKED);
  }

  @Test
  void publishFailureIsCountedNotThrown() {
    MockProducer<String, byte[]> failing = new MockProducer<>(false, new StringSerializer(),
        new ByteArraySerializer());
    LifecyclePublisher p = new LifecyclePublisher(TestKafka.of(failing, f.telemetry), f.topics, f.node, "cell-1",
        f.metrics, () -> 0L);

    p.activated(active());
    failing.errorNext(new RuntimeException("down"));

    assertThat(f.meters.get("gw.lifecycle.publish.errors").counter().count()).isEqualTo(1);
  }
}
