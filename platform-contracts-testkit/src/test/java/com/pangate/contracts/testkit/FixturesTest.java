package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.EventType;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Kick;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import org.junit.jupiter.api.Test;

class FixturesTest {

  static ControlOp kickOp() {
    return ControlOp.newBuilder().setKick(Kick.newBuilder().setNewConnId("01JCNEWCONN0000000000000000")
        .setConnectedAtMs(Fixtures.FIXED_NOW_MS)).build();
  }

  @Test
  void fixturesObeySpecSection11() {
    PushRecord m = Fixtures.message("t1:u1");
    assertThat(m.getKind()).isEqualTo(PushRecord.Kind.KIND_MESSAGE);
    assertThat(m.getTenantId()).isEqualTo("t1");
    assertThat(m.getFrame().getType()).isEqualTo(FrameType.FRAME_TYPE_PUSH);
    assertThat(m.getFrame().getMsgId()).isEqualTo(m.getMsgId()).isEqualTo(Fixtures.FIXED_MSG_ID);
    assertThat(m.getFrame().getClass_()).isEqualTo(m.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_RELIABLE);
    assertThat(m.getFrame().getSeq()).isEqualTo(1);
    assertThat(m.getExpiresAtMs() - m.getAcceptedAtMs()).isEqualTo(86_400_000L);

    PushRecord e = Fixtures.ephemeral("t1:u1");                        // Review Focus 5
    assertThat(e.getFrame().getSeq()).isZero();
    assertThat(e.getFrame().getTopicSeq()).isZero();

    PushRecord b = Fixtures.broadcast("t1:news");                     // Review Focus 5
    assertThat(b.getKind()).isEqualTo(PushRecord.Kind.KIND_BROADCAST);
    assertThat(b.getFrame().getTopic()).isEqualTo(b.getTopic()).isEqualTo("t1:news");
    assertThat(b.getFrame().getTopicSeq()).isEqualTo(1);
    assertThat(b.getFrame().getSeq()).isZero();

    PushRecord c = Fixtures.control("t1:u1", kickOp());
    assertThat(c.getKind()).isEqualTo(PushRecord.Kind.KIND_CONTROL);
    assertThat(c.hasFrame()).isFalse();

    PushRecord dm = Fixtures.deviceMessage("t1:u1", "d1");
    assertThat(dm.getDevice().getDeviceId()).isEqualTo("d1");
    assertThat(dm.getFrame().getSeq()).isZero();     // device targets take no per-principal seq (spec §1.1)
    assertThat(dm.getFrame().getSeqEpoch()).isZero();
  }

  @Test
  void derivedFixtures() {
    PushRecord d = Fixtures.deviceMessage("t1:u1", "d1");
    assertThat(Fixtures.offline(d, OfflineRecord.Reason.REASON_NO_SESSION, 0).getRecord()).isEqualTo(d);
    assertThat(Fixtures.offline(d, OfflineRecord.Reason.REASON_NACK_TIMEOUT, 3).getAttempts()).isEqualTo(3);
    var delivered = Fixtures.delivered(d, "conn-1");
    assertThat(delivered.getPrincipalId()).isEqualTo("t1:u1");
    assertThat(delivered.getDeviceId()).isEqualTo("d1");
    assertThat(Fixtures.nodeDown("gw-1", 7).getType()).isEqualTo(EventType.EVENT_TYPE_NODE_DOWN);
    assertThat(Fixtures.nodeDown("gw-1", 7).getPrincipalId()).isEmpty();
  }

  @Test
  void kickRequestIsWhatGateway010Sends() {
    PushRequest k = Fixtures.kickRequest010();
    assertThat(k.getKind()).isEqualTo(PushRequest.Kind.KIND_CONTROL);
    assertThat(k.getControl().hasKick()).isTrue();
    // only fields 1..5 set
    assertThat(k.getAllFields().keySet()).extracting(f -> f.getNumber()).allMatch(n -> n <= 5);
  }
}
