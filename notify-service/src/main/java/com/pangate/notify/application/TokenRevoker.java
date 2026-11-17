package com.pangate.notify.application;

import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.Revoke;
import com.pangate.notify.application.port.TokenStore;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Offline notifications spec §5.3: a revoked device or principal loses its push tokens. {@code Revoke} carries no
 * tenant, but a device id is a ULID never reused (CH2 §2), so principal and device name one device.
 */
public final class TokenRevoker {

  private final TokenStore store;
  private final MeterRegistry meters;

  public TokenRevoker(TokenStore store, MeterRegistry meters) {
    this.store = store;
    this.meters = meters;
  }

  /** Returns how many tokens were removed. */
  public int apply(ControlEvent e) {
    if (!e.hasRevoke()) {
      return 0;
    }
    Revoke r = e.getRevoke();
    int n = switch (r.getTargetCase()) {
      case DEVICE -> store.removeDevice(r.getDevice().getPrincipalId(), r.getDevice().getDeviceId());
      case PRINCIPAL_ID -> store.removePrincipal(r.getPrincipalId());
      default -> 0;
    };
    if (n > 0) {
      meters.counter("notify.tokens.removed", "reason", "revoked").increment(n);
    }
    return n;
  }
}
