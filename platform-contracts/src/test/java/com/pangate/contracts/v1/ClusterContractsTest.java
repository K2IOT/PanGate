package com.pangate.contracts.v1;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

class ClusterContractsTest {

  @Test
  void pushItemRoundTripsWithFrameAndControl() throws Exception {
    PushItem item = PushItem.newBuilder()
        .setDeliveryId(9)
        .setKind(PushItem.Kind.KIND_CONTROL)
        .setTenantId("t1")
        .setPrincipalId("t1:u1")
        .setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId("m1"))
        .setControl(ControlOp.newBuilder().setKick(Kick.newBuilder().setNewConnId("c2").setConnectedAtMs(5)))
        .setPriority(PushItem.Priority.PRIORITY_CRITICAL)
        .build();

    PushItem parsed = PushItem.parseFrom(PushBatch.newBuilder().addItems(item).build().getItems(0).toByteArray());

    assertThat(parsed).isEqualTo(item);
    assertThat(parsed.getControl().getKick().getNewConnId()).isEqualTo("c2");
    assertThat(AckItem.Status.STATUS_UNSPECIFIED.getNumber()).isZero();
  }

  @Test
  void connectionEventRoundTrips() throws Exception {
    ConnectionEvent e = ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_DISCONNECTED).setConnId("c1")
        .setPrincipalId("t1:u1").setEpoch(7).setCloseCode(4003).setAtMs(123).build();

    ConnectionEvent parsed = ConnectionEvent.parseFrom(e.toByteArray());

    assertThat(parsed.getType()).isEqualTo(EventType.EVENT_TYPE_DISCONNECTED);
    assertThat(parsed.getEpoch()).isEqualTo(7);
    assertThat(parsed.getCloseCode()).isEqualTo(4003);
  }

  @Test
  void controlEventOptionalFieldsTrackPresence() throws Exception {
    TenantCfg c = TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setTenantBlocked(false)).build();

    TenantCfg p = TenantCfg.parseFrom(ControlEvent.newBuilder().setTenantCfg(c).build().getTenantCfg().toByteArray());

    assertThat(p.getSwitches().hasTenantBlocked()).isTrue();
    assertThat(p.getSwitches().hasUpstreamTelemetryEnabled()).isFalse();
    assertThat(p.getLimits().hasMaxConns()).isFalse();
  }

  @Test
  void revokeTargetsAreExclusive() throws Exception {
    Revoke r = Revoke.newBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId("t1:u1").setDeviceId("d1"))
        .setExpiresAtMs(10).build();

    Revoke p = Revoke.parseFrom(r.toByteArray());

    assertThat(p.getTargetCase()).isEqualTo(Revoke.TargetCase.DEVICE);
    assertThat(p.getDevice().getDeviceId()).isEqualTo("d1");
  }

  @Test
  void upstreamRecordKeepsHeadersAndPayload() throws Exception {
    UpstreamRecord u = UpstreamRecord.newBuilder().setMsgId("m1").putHeaders("traceparent", "00-x")
        .setPayload(ByteString.copyFromUtf8("hi")).setReceivedAtMs(1).build();

    UpstreamRecord p = UpstreamRecord.parseFrom(u.toByteArray());

    assertThat(p.getHeadersMap()).containsEntry("traceparent", "00-x");
    assertThat(p.getPayload().toStringUtf8()).isEqualTo("hi");
  }

  @Test
  void pushRequestCarriesControlKick() throws Exception {
    PushRequest r = PushRequest.newBuilder().setRequestId("r1").setKind(PushRequest.Kind.KIND_CONTROL)
        .setPrincipalId("t1:u1").setControl(ControlOp.newBuilder().setKick(Kick.newBuilder().setNewConnId("c9")))
        .build();

    assertThat(PushRequest.parseFrom(r.toByteArray()).getControl().getKick().getNewConnId()).isEqualTo("c9");
  }
}
