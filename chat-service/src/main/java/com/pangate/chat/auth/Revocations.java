package com.pangate.chat.auth;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.ControlEvent;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * The revocations of {@code gw.control} that apply to connect tokens (chat spec §1): {@code revoke:j:<jti>},
 * {@code revoke:d:<principal>/<device>} and {@code revoke:p:<principal>}. A device or principal revocation blocks the
 * tokens issued before it ({@code iat < issued_at_ms}), a jti revocation blocks that token; each until
 * {@code expires_at_ms} (0 = no end). A tombstone (null value) lifts it. Other keys are ignored.
 */
public final class Revocations {

  private record Revocation(long issuedAtMs, long expiresAtMs) {
    boolean live(long now) {
      return expiresAtMs == 0 || now < expiresAtMs;
    }
  }

  static final String JTI = "revoke:j:";
  static final String DEVICE = "revoke:d:";
  static final String PRINCIPAL = "revoke:p:";

  private final Map<String, Revocation> byKey = new ConcurrentHashMap<>();
  private final LongSupplier wallMs;

  public Revocations(LongSupplier wallMs) {
    this.wallMs = wallMs;
  }

  /** Applies one record; throws {@link IllegalArgumentException} for a malformed value of a key it keeps. */
  public void apply(String key, byte[] value) {
    if (key == null || !(key.startsWith(JTI) || key.startsWith(DEVICE) || key.startsWith(PRINCIPAL))) {
      return;
    }
    if (value == null) {
      byKey.remove(key);
      return;
    }
    ControlEvent e;
    try {
      e = ControlEvent.parseFrom(value);
    } catch (InvalidProtocolBufferException ex) {
      throw new IllegalArgumentException("malformed control event for " + key, ex);
    }
    if (!e.hasRevoke()) {
      throw new IllegalArgumentException(key + " is not a revoke event");
    }
    byKey.put(key, new Revocation(e.getIssuedAtMs(), e.getRevoke().getExpiresAtMs()));
  }

  public boolean jtiRevoked(String jti) {
    Revocation r = byKey.get(JTI + jti);
    return r != null && r.live(wallMs.getAsLong());
  }

  /** True when the principal or this device of it was revoked after the token was issued. */
  public boolean sessionRevoked(String principal, String deviceId, long iatMs) {
    long now = wallMs.getAsLong();
    Revocation p = byKey.get(PRINCIPAL + principal);
    if (p != null && p.live(now) && iatMs < p.issuedAtMs()) {
      return true;
    }
    Revocation d = byKey.get(DEVICE + principal + "/" + deviceId);
    return d != null && d.live(now) && iatMs < d.issuedAtMs();
  }

  public int size() {
    return byKey.size();
  }
}
