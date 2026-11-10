package com.pangate.auth.infrastructure.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.domain.Revocation;
import com.pangate.auth.domain.Revocation.Kind;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.Revoke;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ControlEventMapperTest {

  private final ControlEventMapper mapper = new ControlEventMapper();

  @Test
  void keysMatchTheGatewayContract() {
    assertThat(ControlEventMapper.key(Kind.PRINCIPAL, "t1:u1", null, null, null)).isEqualTo("revoke:p:t1:u1");
    assertThat(ControlEventMapper.key(Kind.DEVICE, "t1:u1", "d1", null, null)).isEqualTo("revoke:d:t1:u1/d1");
    assertThat(ControlEventMapper.key(Kind.JTI, null, null, "abc", null)).isEqualTo("revoke:j:abc");
    assertThat(ControlEventMapper.key(Kind.CLIENT, null, null, null, "t1-backend")).isEqualTo("revoke:c:t1-backend");
  }

  private Revoke roundTrip(Kind kind, String key) throws Exception {
    Instant exp = Instant.ofEpochMilli(2_000_000);
    byte[] v = mapper.revoke(new Revocation(1, "t1", kind, key, exp, "t1-backend"), Instant.ofEpochMilli(1_000));
    ControlEvent e = ControlEvent.parseFrom(v);
    assertThat(e.getIssuedAtMs()).isEqualTo(1_000);
    assertThat(e.getRevoke().getExpiresAtMs()).isEqualTo(2_000_000);
    return e.getRevoke();
  }

  @Test
  void valueRoundTripsThroughControlEvent() throws Exception {
    assertThat(roundTrip(Kind.PRINCIPAL, "revoke:p:t1:u1").getPrincipalId()).isEqualTo("t1:u1");
    Revoke d = roundTrip(Kind.DEVICE, "revoke:d:t1:u/x/d1");
    assertThat(d.getDevice().getPrincipalId()).isEqualTo("t1:u/x");
    assertThat(d.getDevice().getDeviceId()).isEqualTo("d1");
    assertThat(roundTrip(Kind.JTI, "revoke:j:abc").getJti()).isEqualTo("abc");
    assertThat(roundTrip(Kind.CLIENT, "revoke:c:t1-backend").getClientId()).isEqualTo("t1-backend");
  }
}
