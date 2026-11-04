package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.PushItem.Priority;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import com.pangate.gateway.test.TestLimits;
import io.netty.buffer.ByteBuf;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BroadcastPacerTest {

  private final LoopFixture f = new LoopFixture();
  private final List<EmbeddedChannel> channels = new ArrayList<>();
  private final List<Session> sessions = new ArrayList<>();
  private BroadcastPacer pacer;

  @BeforeEach
  void setUp() {
    f.limits.set(TestLimits.withBroadcastRate(1000));  // 10 recipients per 10 ms tick
    pacer = new BroadcastPacer(f.loop, f.node, f.tenants, f.limits, f.metrics);
  }

  private void subscribers(int n, String topic) {
    for (int i = 0; i < n; i++) {
      EmbeddedChannel ch = new EmbeddedChannel();
      Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
      s.activate(new Session.Identity("t1", "t1:u" + sessions.size(), "d", "ios", "1.0", Set.of()),
          f.tracker.newConnId(s), 0L, f.wallNow(), f.wallNow() + 3600, 1, List.of());
      f.loop.topics().add(s, topic);
      channels.add(ch);
      sessions.add(s);
    }
  }

  private PreEncodedFrame frame(String topic) {
    return PreEncodedFrame.encode(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setTopic(topic)
        .setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL).build(), "t1", 1);
  }

  /** Reads (and releases) what each subscriber received so far; returns how many channels got a frame. */
  private int received() {
    int n = 0;
    for (EmbeddedChannel ch : channels) {
      Object o;
      boolean got = false;
      while ((o = ch.readOutbound()) != null) {
        got = true;
        ReferenceCountUtil.release(o);
      }
      if (got) {
        n++;
      }
    }
    return n;
  }

  private void tick(long ms) {
    f.channel.advanceTimeBy(ms, TimeUnit.MILLISECONDS);
    f.channel.runScheduledPendingTasks();
  }

  @Test
  void criticalWritesAllRecipientsImmediately() {
    subscribers(25, "t1:x");
    PreEncodedFrame fr = frame("t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_CRITICAL, fr);
    assertThat(received()).isEqualTo(25);
    assertThat(fr.refCnt()).isZero();
    assertThat(f.meters.counter("gw.broadcast.recipients", "priority", "critical").count()).isEqualTo(25);
  }

  @Test
  void normalIsPacedPerTick() {
    subscribers(25, "t1:x");
    PreEncodedFrame fr = frame("t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, fr);
    assertThat(received()).isZero();
    tick(10);
    assertThat(received()).isEqualTo(10);
    tick(10);
    assertThat(received()).isEqualTo(10);
    tick(10);
    assertThat(received()).isEqualTo(5);
    assertThat(fr.refCnt()).isZero();
    assertThat(pacer.queued()).isZero();
  }

  @Test
  void unspecifiedPriorityIsNormal() {
    subscribers(3, "t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_UNSPECIFIED, frame("t1:x"));
    tick(10);
    assertThat(received()).isEqualTo(3);
  }

  @Test
  void normalBeforeBulk() {
    subscribers(10, "t1:x");
    PreEncodedFrame bulk = frame("t1:x");
    PreEncodedFrame normal = frame("t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_BULK, bulk);
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, normal);
    tick(10);
    received();
    assertThat(normal.refCnt()).isZero();
    assertThat(bulk.refCnt()).isEqualTo(1);
    tick(10);
    received();
    assertThat(bulk.refCnt()).isZero();
  }

  @Test
  void bulkPausedWhileShedding() {
    subscribers(5, "t1:x");
    f.node.shedLevel(1);
    PreEncodedFrame bulk = frame("t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_BULK, bulk);
    tick(10);
    tick(100);
    assertThat(received()).isZero();
    assertThat(pacer.queued()).isEqualTo(1);
    f.node.shedLevel(0);
    tick(100);
    assertThat(received()).isEqualTo(5);
    assertThat(bulk.refCnt()).isZero();
  }

  @Test
  void broadcastDisabledDropsNonCritical() {
    subscribers(3, "t1:x");
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setBroadcastEnabled(false)).build());
    PreEncodedFrame normal = frame("t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, normal);
    assertThat(normal.refCnt()).isZero();
    assertThat(f.meters.counter("gw.push.dropped", "reason", "broadcast_disabled").count()).isEqualTo(1);
    pacer.submit("t1:x", Priority.PRIORITY_CRITICAL, frame("t1:x"));
    assertThat(received()).isEqualTo(3);
  }

  @Test
  void unwritableRecipientSkipped() {
    subscribers(3, "t1:x");
    EmbeddedChannel slow = channels.get(1);
    slow.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1, 2));
    slow.write(io.netty.buffer.Unpooled.wrappedBuffer(new byte[64]));  // unflushed: channel stays unwritable
    assertThat(slow.isWritable()).isFalse();
    PreEncodedFrame fr = frame("t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_CRITICAL, fr);
    assertThat(slow.outboundMessages()).isEmpty();
    assertThat(channels.get(0).outboundMessages()).hasSize(1);
    assertThat(channels.get(2).outboundMessages()).hasSize(1);
    received();
    assertThat(fr.refCnt()).isZero();
  }

  @Test
  void closedRecipientIsSkippedAndBufferReleased() {
    subscribers(15, "t1:x");
    PreEncodedFrame fr = frame("t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, fr);
    tick(10);
    assertThat(received()).isEqualTo(10);
    sessions.get(12).close(CloseCode.NORMAL, "bye");
    channels.get(12).runPendingTasks();
    received();
    tick(10);
    assertThat(received()).isEqualTo(4);
    assertThat(fr.refCnt()).isZero();
  }

  @Test
  void loopWithoutSubscribersReleasesItsShare() {
    PreEncodedFrame fr = frame("t1:nobody");
    pacer.submit("t1:nobody", Priority.PRIORITY_NORMAL, fr);
    assertThat(fr.refCnt()).isZero();
    assertThat(pacer.queued()).isZero();
  }

  @Test
  void noTimerWhenIdle() {
    subscribers(2, "t1:x");
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, frame("t1:x"));
    tick(10);
    received();
    assertThat(pacer.scheduled()).isFalse();
  }

  @Test
  void backlogBeyondCapDropsOldestBulkThenOldestNormal() {
    subscribers(1, "t1:x");
    List<PreEncodedFrame> bulk = new ArrayList<>();
    List<PreEncodedFrame> normal = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      PreEncodedFrame fr = frame("t1:x");
      bulk.add(fr);
      pacer.submit("t1:x", Priority.PRIORITY_BULK, fr);
    }
    for (int i = 0; i < BroadcastPacer.MAX_QUEUED_JOBS - 2; i++) {
      PreEncodedFrame fr = frame("t1:x");
      normal.add(fr);
      pacer.submit("t1:x", Priority.PRIORITY_NORMAL, fr);
    }
    assertThat(pacer.queued()).isEqualTo(BroadcastPacer.MAX_QUEUED_JOBS);
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, frame("t1:x"));   // drops the oldest BULK
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, frame("t1:x"));   // drops the next BULK
    pacer.submit("t1:x", Priority.PRIORITY_NORMAL, frame("t1:x"));   // no BULK left: drops the oldest NORMAL
    assertThat(pacer.queued()).isEqualTo(BroadcastPacer.MAX_QUEUED_JOBS);
    assertThat(bulk).allMatch(fr -> fr.refCnt() == 0);
    assertThat(normal.get(0).refCnt()).isZero();
    assertThat(normal.get(1).refCnt()).isEqualTo(1);
    assertThat(f.meters.counter("gw.push.dropped", "reason", "broadcast_backlog").count()).isEqualTo(3);
  }
}
