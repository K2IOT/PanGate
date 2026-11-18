package com.pangate.notify.application.port;

import com.pangate.notify.domain.PushToken;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Push tokens per device (offline notifications spec §5.2). May throw {@link StoreUnavailableException}. */
public interface TokenStore {

  /**
   * In one transaction: removes the same token from any other device (invariant 3), then sets it on this device.
   * Returns how many other devices lost it.
   */
  int register(PushToken t);

  void remove(String tenant, String principal, String device);

  List<PushToken> byPrincipal(String tenant, String principal);

  Optional<PushToken> byDevice(String tenant, String principal, String device);

  /** Every tenant: a revocation names principal and device only (spec §5.3). */
  int removeDevice(String principal, String device);

  int removePrincipal(String principal);

  /** Removes the device's row only while it still holds {@code token} (spec §5.4: a dead token, not a newer one). */
  boolean removeIfToken(String tenant, String principal, String device, String token);

  int purgeIdle(Instant before);
}
