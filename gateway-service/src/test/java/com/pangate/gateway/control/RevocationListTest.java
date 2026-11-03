package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Revoke;
import com.pangate.gateway.auth.ResumeTicketService;
import org.junit.jupiter.api.Test;

class RevocationListTest {

  private final RevocationList list = new RevocationList();

  @Test
  void revokedByPrincipalDeviceOrJti() {
    long jti = ResumeTicketService.jtiHash("jti-1");
    list.add(RevocationList.principalKey("t1:u1"), 1_000);
    list.add(RevocationList.deviceKey("t1:u2", "d2"), 1_000);
    list.add(RevocationList.jtiKey(jti), 1_000);

    assertThat(list.isRevoked("t1:u1", "dx", 0, 500)).isTrue();
    assertThat(list.isRevoked("t1:u2", "d2", 0, 500)).isTrue();
    assertThat(list.isRevoked("t1:u3", "d3", jti, 500)).isTrue();
    assertThat(list.isRevoked("t1:u3", "d3", 7, 500)).isFalse();
  }

  @Test
  void expiredEntriesIgnoredAndSwept() {
    list.add(RevocationList.principalKey("t1:u1"), 1_000);

    assertThat(list.isRevoked("t1:u1", "d", 0, 1_000)).isFalse();
    assertThat(list.sweep(1_000)).isEqualTo(1);
    assertThat(list.size()).isZero();
  }

  @Test
  void deviceRevokeScopedToPrincipal() {
    list.add(RevocationList.deviceKey("t1:u1", "d1"), Long.MAX_VALUE);

    assertThat(list.isRevoked("t1:u2", "d1", 0, 0)).isFalse();
  }

  @Test
  void removeLiftsRevocation() {
    list.add(RevocationList.principalKey("t1:u1"), Long.MAX_VALUE);
    list.remove(RevocationList.principalKey("t1:u1"));

    assertThat(list.isRevoked("t1:u1", "d", 0, 0)).isFalse();
  }

  @Test
  void keyOfRevokeEvent() {
    assertThat(RevocationList.keyOf(Revoke.newBuilder().setPrincipalId("t1:u1").build())).hasValue("p:t1:u1");
    assertThat(RevocationList.keyOf(Revoke.newBuilder()
        .setDevice(DeviceRef.newBuilder().setPrincipalId("t1:u1").setDeviceId("d1")).build())).hasValue("d:t1:u1/d1");
    assertThat(RevocationList.keyOf(Revoke.newBuilder().setJti("j").build()))
        .hasValue("j:" + ResumeTicketService.jtiHash("j"));
    assertThat(RevocationList.keyOf(Revoke.getDefaultInstance())).isEmpty();
  }
}
