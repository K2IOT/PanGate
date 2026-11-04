package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.Ack;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.WriteBufferWaterMark;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PushDeliveryTest {

  private final LoopFixture f = new LoopFixture();
  private final List<AckItem> acks = new ArrayList<>();
  private final StreamRef stream = new StreamRef(acks::add);
  private Session session;
  private String connId;

  @BeforeEach
  void activeSession() {
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    connId = f.tracker.newConnId(session);
    session.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "1.0", Set.of()), connId, 0L,
        f.wallNow(), f.wallNow() + 3_600, 7, List.of());
    f.tracker.onActivated(session);
  }

  private PushItem item(long deliveryId, DeliveryClass cls, String msgId, int payloadBytes) {
    return PushItem.newBuilder().setDeliveryId(deliveryId).setKind(PushItem.Kind.KIND_MESSAGE).setTenantId("t1")
        .setPrincipalId("t1:u1").setConnId(connId)
        .setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId(msgId).setClass_(cls)
            .setPayload(ByteString.copyFrom(new byte[payloadBytes])))
        .build();
  }

  private void deliver(PushItem i) {
    io.netty.channel.Channel written = f.delivery.deliver(f.loop, i, stream);
    if (written != null) {
      written.flush();  // LoopBatchTask flushes the channels written in its task
    }
    f.channel.runPendingTasks();
  }

  private List<Frame> written() {
    List<Frame> out = new ArrayList<>();
    Object o;
    while ((o = f.channel.readOutbound()) != null) {
      if (o instanceof Frame fr) {
        out.add(fr);
      }
    }
    return out;
  }

  private static AckItem ack(long id, AckItem.Status s) {
    return AckItem.newBuilder().setDeliveryId(id).setStatus(s).build();
  }

  private void clientAck(String... ids) {
    f.delivery.onAck(session, Ack.newBuilder().addAllMsgIds(List.of(ids)).build());
  }

  @Test
  void reliablePushIsWrittenAndAckedOnClientAck() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 10));
    assertThat(written()).extracting(Frame::getMsgId).containsExactly("m1");
    assertThat(acks).isEmpty();
    assertThat(f.loop.pendingInflight()).isEqualTo(1);

    clientAck("m1");

    assertThat(acks).containsExactly(ack(1, AckItem.Status.STATUS_ACKED));
    assertThat(session.pending()).isNull();
    assertThat(f.loop.pendingInflight()).isZero();
  }

  @Test
  void ephemeralIsWrittenWithoutAck() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "m1", 10));
    assertThat(written()).hasSize(1);
    assertThat(acks).isEmpty();
    assertThat(session.pending()).isNull();
  }

  @Test
  void ephemeralToUnwritableChannelIsDropped() {
    f.channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1, 2));
    f.channel.write(io.netty.buffer.Unpooled.wrappedBuffer(new byte[1024]));
    assertThat(f.channel.isWritable()).isFalse();
    written();

    deliver(item(1, DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "m1", 10));

    assertThat(written()).isEmpty();
    assertThat(f.meters.counter("gw.push.dropped", "reason", "unwritable").count()).isEqualTo(1);
  }

  @Test
  void wrongConnIdForSameKeyIsNotFound() {
    String other = connId.substring(0, 25) + (connId.charAt(25) == '0' ? '1' : '0');
    PushItem i = item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1).toBuilder().setConnId(other).build();
    deliver(i);
    assertThat(written()).isEmpty();
    assertThat(acks).containsExactly(ack(1, AckItem.Status.STATUS_NACK_NOT_FOUND));
  }

  @Test
  void closedSessionReliableNotFoundEphemeralDropped() {
    session.close(CloseCode.NORMAL, "bye");
    f.channel.runPendingTasks();
    written();
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    deliver(item(2, DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "m2", 1));
    assertThat(acks).containsExactly(ack(1, AckItem.Status.STATUS_NACK_NOT_FOUND));
    assertThat(f.meters.counter("gw.push.dropped", "reason", "not_found").count()).isEqualTo(1);
  }

  @Test
  void duplicateMsgIdFromNewStreamRetargetsEntryWithoutRewrite() {
    List<AckItem> oldAcks = new ArrayList<>();
    StreamRef old = new StreamRef(oldAcks::add);
    f.delivery.deliver(f.loop, item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1), old);
    old.markDead();  // router failed over to a new stream and replays
    deliver(item(2, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    f.channel.flush();
    assertThat(written()).hasSize(1);
    clientAck("m1");
    assertThat(acks).containsExactly(ack(2, AckItem.Status.STATUS_ACKED));
    assertThat(oldAcks).isEmpty();
  }

  @Test
  void duplicateMsgIdOnSameStreamAnswersBothDeliveryIds() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    deliver(item(2, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    assertThat(written()).hasSize(1);
    clientAck("m1");
    assertThat(acks).containsExactlyInAnyOrder(ack(1, AckItem.Status.STATUS_ACKED),
        ack(2, AckItem.Status.STATUS_ACKED));
  }

  @Test
  void duplicateOnSameStreamTimesOutBothDeliveryIds() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    deliver(item(2, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    f.advanceTo(f.now() + 5);
    assertThat(acks).containsExactlyInAnyOrder(ack(1, AckItem.Status.STATUS_NACK_TIMEOUT),
        ack(2, AckItem.Status.STATUS_NACK_TIMEOUT));
  }

  @Test
  void overflowNacksAndClosesAfter64In30s() {
    for (int i = 0; i < 32; i++) {
      deliver(item(i, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m" + i, 1));
    }
    for (int i = 32; i < 96; i++) {
      deliver(item(i, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m" + i, 1));
    }
    f.channel.runPendingTasks();

    assertThat(acks.stream().filter(a -> a.getStatus() == AckItem.Status.STATUS_NACK_OVERFLOW)).hasSize(64);
    assertThat(session.state()).isEqualTo(SessionState.CLOSED);
    assertThat(session.closeCode()).isEqualTo(CloseCode.SLOW_CONSUMER);
    assertThat(acks.stream().filter(a -> a.getStatus() == AckItem.Status.STATUS_NACK_SLOW_CONSUMER)).hasSize(32);
  }

  @Test
  void overflowCountResetsAfter30s() {
    for (int i = 0; i < 32; i++) {
      deliver(item(i, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m" + i, 1));
    }
    for (int i = 32; i < 95; i++) {
      deliver(item(i, DeliveryClass.DELIVERY_CLASS_RELIABLE, "x" + i, 1));
    }
    f.clockSec.addAndGet(31);
    clientAck("m0");  // window keeps 31 entries; timeouts are not run (no wheel advance)
    deliver(item(200, DeliveryClass.DELIVERY_CLASS_RELIABLE, "y", 1));
    deliver(item(201, DeliveryClass.DELIVERY_CLASS_RELIABLE, "z", 1));
    assertThat(session.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void byteLimitAlsoOverflows() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 40_000));
    deliver(item(2, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m2", 40_000));
    assertThat(acks).containsExactly(ack(2, AckItem.Status.STATUS_NACK_OVERFLOW));
  }

  @Test
  void oversizedFrameIsAcceptedIntoAnEmptyWindow() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "big", 70_000));
    assertThat(written()).hasSize(1);
    assertThat(acks).isEmpty();
  }

  @Test
  void downgradeReliableActsEphemeralAndAnswersWritten() {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_CELL).setScopeId("cell-1")
        .setSwitches(KillSwitches.newBuilder().setDeliveryDowngradeReliable(true)).build());
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    assertThat(written()).hasSize(1);
    assertThat(acks).containsExactly(ack(1, AckItem.Status.STATUS_WRITTEN));
    assertThat(session.pending()).isNull();
  }

  @Test
  void pendingTimesOutAfter5sWithoutClosing() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    f.advanceTo(f.now() + 4);
    assertThat(acks).isEmpty();
    f.advanceTo(f.now() + 1);
    assertThat(acks).containsExactly(ack(1, AckItem.Status.STATUS_NACK_TIMEOUT));
    assertThat(session.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(session.pending()).isNull();
    assertThat(f.loop.pendingInflight()).isZero();
  }

  @Test
  void ackAfterTimeoutSendsNothing() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    f.advanceTo(f.now() + 5);
    acks.clear();
    clientAck("m1");
    assertThat(acks).isEmpty();
  }

  @Test
  void unknownAndDuplicateAckIdsAreIgnored() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    clientAck("nope", "m1", "m1");
    clientAck("m1");
    assertThat(acks).containsExactly(ack(1, AckItem.Status.STATUS_ACKED));
  }

  @Test
  void ackWithMoreThan32IdsIsBadFrame() {
    Ack.Builder b = Ack.newBuilder();
    for (int i = 0; i < 33; i++) {
      b.addMsgIds("m" + i);
    }
    assertThatThrownBy(() -> f.delivery.onAck(session, b.build()))
        .isInstanceOfSatisfying(ProtocolException.class, e -> assertThat(e.code()).isEqualTo(CloseCode.BAD_FRAME));
  }

  @Test
  void closeNacksRemainingPendingAsClosed() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", 1));
    f.channel.close();
    f.channel.runPendingTasks();
    assertThat(acks).containsExactly(ack(1, AckItem.Status.STATUS_NACK_CLOSED));
    assertThat(f.loop.pendingInflight()).isZero();
  }

  @Test
  void handoffRecordedWhenEnqueuedAtSet() {
    deliver(item(1, DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "m1", 1).toBuilder()
        .setEnqueuedAtMs(f.wallNow() * 1000 - 250).build());
    assertThat(f.meters.timer("gw.push.handoff").count()).isEqualTo(1);
  }
}
