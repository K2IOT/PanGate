package com.pangate.auth.infrastructure.kafka;

import com.pangate.auth.application.port.ControlEncoder;
import com.pangate.auth.domain.Revocation;
import com.pangate.auth.domain.Revocation.Kind;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Revoke;
import java.time.Instant;

/** gw.control keys and {@code ControlEvent{revoke}} values (contract in control.proto). */
public final class ControlEventMapper implements ControlEncoder {

  public static String key(Kind kind, String principal, String device, String jti, String clientId) {
    return Revocation.controlKey(kind, principal, device, jti, clientId);
  }

  /** The target is read back from the control key, so the key and the value can never disagree. */
  @Override
  public byte[] revoke(Revocation r, Instant issuedAt) {
    String rest = r.controlKey().substring("revoke:x:".length());
    Revoke.Builder b = Revoke.newBuilder().setExpiresAtMs(r.expiresAt().toEpochMilli());
    switch (r.kind()) {
      case PRINCIPAL -> b.setPrincipalId(rest);
      case DEVICE -> {
        int slash = rest.lastIndexOf('/');
        b.setDevice(DeviceRef.newBuilder().setPrincipalId(rest.substring(0, slash))
            .setDeviceId(rest.substring(slash + 1)));
      }
      case JTI -> b.setJti(rest);
      case CLIENT -> b.setClientId(rest);
    }
    return ControlEvent.newBuilder().setRevoke(b).setIssuedAtMs(issuedAt.toEpochMilli()).build().toByteArray();
  }
}
