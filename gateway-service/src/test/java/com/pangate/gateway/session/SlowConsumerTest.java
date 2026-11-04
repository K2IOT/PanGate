package com.pangate.gateway.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.downstream.StreamRef;
import com.pangate.gateway.pipeline.WritabilityTracker;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.Unpooled;
import io.netty.channel.WriteBufferWaterMark;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SlowConsumerTest {

  private final LoopFixture f = new LoopFixture();
  private final List<AckItem> acks = new ArrayList<>();
  private Session session;

  @BeforeEach
  void activeSessionWithPendingPush() {
    f.metrics.bindLoops(f.loops);
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    String connId = f.tracker.newConnId(session);
    session.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "1.0", Set.of()), connId, 0L,
        f.wallNow(), f.wallNow() + 3_600, 7, List.of());
    f.channel.pipeline().addLast(WritabilityTracker.INSTANCE);
    f.delivery.deliver(f.loop, PushItem.newBuilder().setDeliveryId(1).setKind(PushItem.Kind.KIND_MESSAGE)
        .setConnId(connId).setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId("m1")
            .setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE)).build(), new StreamRef(acks::add));
    f.channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1, 2));
  }

  private void becomeUnwritable() {
    f.channel.write(Unpooled.wrappedBuffer(new byte[64]));  // unflushed: over the high water mark
    f.channel.runPendingTasks();
    assertThat(f.channel.isWritable()).isFalse();
  }

  private void becomeWritable() {
    f.channel.flush();
    f.channel.runPendingTasks();
    f.channel.releaseOutbound();
    assertThat(f.channel.isWritable()).isTrue();
  }

  private double unwritableGauge() {
    return f.meters.get("gw.unwritable.channels").gauge().value();
  }

  @Test
  void unwritableFor30sClosesWith4008AndNacksSlowConsumer() {
    long t0 = f.now();
    becomeUnwritable();
    assertThat(unwritableGauge()).isEqualTo(1);
    f.advanceTo(t0 + 29);
    assertThat(session.state()).isNotEqualTo(SessionState.CLOSED);
    f.advanceTo(t0 + 30);
    assertThat(session.state()).isEqualTo(SessionState.CLOSED);
    assertThat(session.closeCode()).isEqualTo(CloseCode.SLOW_CONSUMER);
    assertThat(f.meters.counter("gw.slow.consumer.closes", "reason", "unwritable").count()).isEqualTo(1);
    assertThat(unwritableGauge()).isZero();
  }

  @Test
  void slowConsumerCloseNacksPendingAsSlowConsumer() {
    // a fresh push right before the deadline, so the pending-ack timeout has not answered it yet
    long t0 = f.now();
    becomeUnwritable();
    f.advanceTo(t0 + 26);
    acks.clear();
    f.delivery.deliver(f.loop, PushItem.newBuilder().setDeliveryId(2).setKind(PushItem.Kind.KIND_MESSAGE)
        .setConnId(session.connId()).setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId("m2")
            .setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE)).build(), new StreamRef(acks::add));
    f.advanceTo(t0 + 30);
    assertThat(acks).extracting(AckItem::getStatus).containsExactly(AckItem.Status.STATUS_NACK_SLOW_CONSUMER);
  }

  @Test
  void writableAgainClearsTheDeadline() {
    long t0 = f.now();
    becomeUnwritable();
    f.advanceTo(t0 + 20);
    becomeWritable();
    assertThat(unwritableGauge()).isZero();
    f.advanceTo(t0 + 40);
    assertThat(session.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void closeWhileUnwritableResetsTheGauge() {
    becomeUnwritable();
    session.close(CloseCode.NORMAL, "bye");
    f.channel.runPendingTasks();
    assertThat(unwritableGauge()).isZero();
  }

  @Test
  void pendingOverflowCloseIsCountedAsOverflow() {
    for (int i = 2; i < 200 && session.state() != SessionState.CLOSED; i++) {
      f.delivery.deliver(f.loop, PushItem.newBuilder().setDeliveryId(i).setKind(PushItem.Kind.KIND_MESSAGE)
          .setConnId(session.connId()).setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH)
              .setMsgId("m" + i).setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE)).build(),
          new StreamRef(acks::add));
    }
    assertThat(session.closeCode()).isEqualTo(CloseCode.SLOW_CONSUMER);
    assertThat(f.meters.counter("gw.slow.consumer.closes", "reason", "overflow").count()).isEqualTo(1);
  }
}
