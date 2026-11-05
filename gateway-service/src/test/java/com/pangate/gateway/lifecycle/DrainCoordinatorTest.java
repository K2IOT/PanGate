package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.downstream.StreamRef;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DrainCoordinatorTest {

  private final LoopFixture f = new LoopFixture();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final AtomicInteger redisMarks = new AtomicInteger();
  private final AtomicInteger listenerCloses = new AtomicInteger();
  private final List<Session> sessions = new ArrayList<>();

  @AfterEach
  void stop() {
    scheduler.shutdownNow();
  }

  private final List<Long> delayBounds = new ArrayList<>();

  private DrainCoordinator coordinator(int batchPercent, Duration maxDuration, Duration listenerDelay) {
    return new DrainCoordinator(new GatewayProperties.Drain(batchPercent, Duration.ofSeconds(1),
        Duration.ofMillis(500), maxDuration, listenerDelay), f.loops, f.node, redisMarks::incrementAndGet, scheduler,
        f.metrics, bound -> {
          delayBounds.add(bound);
          return 0;
        });
  }

  private void open(int n) {
    for (int i = 0; i < n; i++) {
      EmbeddedChannel ch = new EmbeddedChannel();
      Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
      s.activate(new Session.Identity("t1", "t1:u" + i, "d", "ios", "1.0", Set.of()), f.tracker.newConnId(s), 0L,
          f.wallNow(), f.wallNow() + 3600, 1, List.of());
      sessions.add(s);
    }
  }

  private void advance(long millis) {
    f.channel.advanceTimeBy(millis, TimeUnit.MILLISECONDS);
    f.channel.runScheduledPendingTasks();
    f.channel.runPendingTasks();
  }

  private CompletableFuture<Void> start(DrainCoordinator d) {
    CompletableFuture<Void> done = d.drain(listenerCloses::incrementAndGet);
    f.channel.runPendingTasks();
    return done;
  }

  private double reconnects() {
    return f.meters.counter("gw.drain.reconnects").count();
  }

  @Test
  void drainMarksNodeAndRedisAndClosesListenersAfterDelay() {
    open(1);
    start(coordinator(2, Duration.ofMinutes(10), Duration.ofMillis(50)));
    assertThat(f.node.draining()).isTrue();
    assertThat(redisMarks.get()).isEqualTo(1);
    await().atMost(Duration.ofSeconds(2)).until(() -> listenerCloses.get() == 1);
  }

  @Test
  void eachIntervalReconnectsBatchPercentOfActiveAtStart() {
    open(10);
    start(coordinator(20, Duration.ofMinutes(10), Duration.ZERO));
    assertThat(reconnects()).isEqualTo(2);  // first batch right away
    advance(1000);
    assertThat(reconnects()).isEqualTo(4);
    assertThat(sessions.stream().filter(s -> s.state() == SessionState.DRAINING)).hasSize(4);
  }

  @Test
  void completesWhenNoSessionsRemain() {
    open(2);
    CompletableFuture<Void> done = start(coordinator(100, Duration.ofMinutes(10), Duration.ZERO));
    assertThat(done).isNotDone();
    sessions.forEach(s -> s.channel().close());  // clients reconnect elsewhere
    advance(1000);
    assertThat(done).isCompleted();
  }

  @Test
  void maxDurationForceClosesWith1001() {
    open(1);
    Session s = sessions.get(0);
    List<AckItem> acks = new ArrayList<>();
    f.delivery.deliver(f.loop, PushItem.newBuilder().setDeliveryId(1).setKind(PushItem.Kind.KIND_MESSAGE)
        .setConnId(s.connId()).setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId("m1")
            .setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE)).build(), new StreamRef(acks::add));
    CompletableFuture<Void> done = start(coordinator(2, Duration.ofSeconds(3), Duration.ZERO));
    advance(2000);
    assertThat(s.state()).isEqualTo(SessionState.DRAINING);  // ignores RECONNECT
    advance(1000);
    assertThat(s.state()).isEqualTo(SessionState.CLOSED);
    assertThat(s.closeCode()).isEqualTo(CloseCode.GOING_AWAY);
    assertThat(s.closeReason()).isEqualTo("DRAIN_TIMEOUT");
    assertThat(acks).extracting(AckItem::getStatus).containsExactly(AckItem.Status.STATUS_NACK_CLOSED);
    advance(200);
    assertThat(done).isCompleted();
  }

  @Test
  void secondCallReturnsTheSameFuture() {
    open(1);
    DrainCoordinator d = coordinator(2, Duration.ofMinutes(10), Duration.ZERO);
    CompletableFuture<Void> first = start(d);
    assertThat(d.drain(listenerCloses::incrementAndGet)).isSameAs(first);
    assertThat(redisMarks.get()).isEqualTo(1);
  }

  @Test
  void emptyNodeCompletesImmediately() {
    CompletableFuture<Void> done = start(coordinator(2, Duration.ofMinutes(10), Duration.ZERO));
    assertThat(done).isCompleted();
  }

  @Test
  void reconnectDelaysComeFromTheInjectedSourceBoundedByMaxDelay() {
    open(3);
    start(coordinator(100, Duration.ofMinutes(10), Duration.ZERO));
    assertThat(delayBounds).containsExactly(500L, 500L, 500L);
  }
}
