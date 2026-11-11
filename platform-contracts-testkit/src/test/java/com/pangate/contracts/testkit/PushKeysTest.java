package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.EventType;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import org.junit.jupiter.api.Test;

class PushKeysTest {

  @Test
  void pushRecordKeys() {
    assertThat(PushKeys.of(PushRecord.newBuilder().setKind(PushRecord.Kind.KIND_MESSAGE).setPrincipalId("t1:u1")
        .build())).isEqualTo("t1:u1");
    assertThat(PushKeys.of(PushRecord.newBuilder().setKind(PushRecord.Kind.KIND_MESSAGE)
        .setDevice(DeviceRef.newBuilder().setPrincipalId("t1:u1").setDeviceId("d1")).build())).isEqualTo("t1:u1");
    assertThat(PushKeys.of(PushRecord.newBuilder().setKind(PushRecord.Kind.KIND_BROADCAST).setTopic("t1:news")
        .build())).isEqualTo("topic:t1:news");
    assertThat(PushKeys.of(PushRecord.newBuilder().setKind(PushRecord.Kind.KIND_CONTROL).setPrincipalId("t1:u2")
        .setControl(ControlOp.getDefaultInstance()).build())).isEqualTo("t1:u2");
    assertThatThrownBy(() -> PushKeys.of(PushRecord.newBuilder().setMsgId("m").build()))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("target");
  }

  @Test
  void derivedRecordKeys() {
    PushRecord r = PushRecord.newBuilder().setPrincipalId("t1:u1").build();
    assertThat(PushKeys.of(OfflineRecord.newBuilder().setRecord(r).build())).isEqualTo("t1:u1");
    assertThat(PushKeys.of(DeliveredRecord.newBuilder().setMsgId("m1").setSenderPrincipalId("t1:s").build()))
        .isEqualTo("t1:s");
    assertThat(PushKeys.of(DeliveredRecord.newBuilder().setMsgId("m1").build())).isEqualTo("m1");
  }

  @Test
  void lifecycleKeys() {
    assertThat(PushKeys.of(ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_NODE_DOWN).setNodeId("gw-1")
        .build())).isEqualTo("gw-1");
    assertThat(PushKeys.of(ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_CONNECTED).setNodeId("gw-1")
        .setPrincipalId("t1:u1").build())).isEqualTo("t1:u1");
  }
}
