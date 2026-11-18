package com.pangate.notify.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.CellMap;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Revoke;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/** Offline notifications spec §5.3: which control events remove tokens. */
class TokenRevokerTest {

  final FakeTokenStore store = new FakeTokenStore();
  final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  final TokenRevoker revoker = new TokenRevoker(store, meters);

  static ControlEvent revoke(Revoke.Builder r) {
    return ControlEvent.newBuilder().setIssuedAtMs(1).setRevoke(r).build();
  }

  @Test
  void deviceRevocationRemovesThatDevice() {
    revoker.apply(revoke(Revoke.newBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId("chat:a")
        .setDeviceId("d1"))));
    assertThat(store.calls).containsExactly("removeDevice chat:a/d1");
  }

  @Test
  void principalRevocationRemovesEveryDevice() {
    revoker.apply(revoke(Revoke.newBuilder().setPrincipalId("chat:a")));
    assertThat(store.calls).containsExactly("removePrincipal chat:a");
  }

  @Test
  void otherEventsRemoveNothing() {
    assertThat(revoker.apply(revoke(Revoke.newBuilder().setJti("j1")))).isZero();
    assertThat(revoker.apply(revoke(Revoke.newBuilder().setClientId("c1")))).isZero();
    assertThat(revoker.apply(ControlEvent.newBuilder().setCellMap(CellMap.getDefaultInstance()).build())).isZero();
    assertThat(store.calls).isEmpty();
  }
}
