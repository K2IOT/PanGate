package com.pangate.gateway.test;

import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.lifecycle.TestKafka;
import com.pangate.gateway.lifecycle.TopicNames;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.upstream.UpstreamPublisher;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.session.SessionListener;
import com.pangate.gateway.session.SessionTracker;
import com.pangate.gateway.shed.IpConnectionCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/** One EmbeddedChannel bound to a LoopContext with a fake clock, plus the tracking collaborators. */
public final class LoopFixture {

  /** Monotonic seconds: drives the wheel and session deadlines. */
  public final AtomicLong clockSec = new AtomicLong(1_762_000_000L);
  /** Wall-clock epoch seconds: token and ticket times. Moves with clockSec unless a test makes it jump. */
  public final AtomicLong wallOffset = new AtomicLong();
  public final EmbeddedChannel channel;
  public final LoopContext loop;
  public final LoopRegistry loops;
  public final LimitsHolder limits = TestLimits.holder();
  public final PrincipalIndex principals = new PrincipalIndex();
  public final IpConnectionCounter ips = new IpConnectionCounter();
  public final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  public final GatewayMetrics metrics = new GatewayMetrics(meters);
  /** Listeners the tracker notifies; tests add to it before admitting sessions. */
  public final List<SessionListener> listeners = new CopyOnWriteArrayList<>();
  public final SessionTracker tracker;
  public final NodeState node = new NodeState();
  public final TenantLimitsStore tenants = new TenantLimitsStore("cell-1", () -> limits.current());
  public final TopicNames topics = new TopicNames("c1", List.of("telemetry", "cmd", "cmd_result", "read"));
  /** Not auto-completing: tests call completeNext() / errorNext(). */
  public final MockProducer<String, byte[]> reliable =
      new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
  public final MockProducer<String, byte[]> telemetry =
      new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
  public final UpstreamPublisher upstream;

  public LoopFixture() {
    channel = new EmbeddedChannel();
    loop = new LoopContext(0, channel.eventLoop(), 128, clockSec::get, () -> clockSec.get() + wallOffset.get());
    loops = LoopRegistry.fromContexts(List.of(loop));
    tracker = new SessionTracker(loops, principals, ips, limits, metrics, () -> clockSec.get() * 1000,
        listeners);
    node.nodeId("n1");
    node.epoch(7);
    upstream = new UpstreamPublisher(TestKafka.of(reliable, telemetry), topics, node, "cell-1", tenants, metrics,
        () -> clockSec.get() * 1000);
  }

  /** Moves the clock and runs the wheel as the per-second ticker would. */
  public void advanceTo(long sec) {
    clockSec.set(sec);
    loop.advance(sec);
    channel.runPendingTasks();
  }

  public long now() {
    return clockSec.get();
  }

  public long wallNow() {
    return clockSec.get() + wallOffset.get();
  }
}
