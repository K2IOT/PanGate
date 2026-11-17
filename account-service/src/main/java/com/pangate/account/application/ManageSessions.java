package com.pangate.account.application;

import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.Session;
import java.time.Instant;
import java.util.List;

/**
 * The caller's device sessions (account spec §2, §4.2). Logging a device out revokes its session and sends a
 * {@code device} revocation to auth-service in the same transaction; never a {@code principal} one (ADR 0036).
 */
public final class ManageSessions {

  public record SessionView(String deviceId, String deviceName, String platform, Instant createdAt,
      Instant lastUsedAt, boolean current) {}

  private final SessionStore sessions;
  private final Outbox outbox;
  private final Transactions tx;

  public ManageSessions(SessionStore sessions, Outbox outbox, Transactions tx) {
    this.sessions = sessions;
    this.outbox = outbox;
    this.tx = tx;
  }

  public List<SessionView> list(Caller c) {
    return sessions.live(c.userId()).stream().map((Session s) -> new SessionView(s.deviceId(), s.deviceName(),
        s.platform(), s.createdAt(), s.lastUsedAt(), s.deviceId().equals(c.deviceId()))).toList();
  }

  public void logout(Caller c, String deviceId) {
    tx.inTx(() -> {
      if (!sessions.revoke(c.userId(), deviceId)) {
        throw new DomainException("not_found", "no such session");
      }
      outbox.revokeDevice(c.userId(), deviceId);
    });
  }

  /** Every session of the user, or every other one; returns the revoked device ids. */
  public List<String> logoutAll(String userId, String exceptDeviceId) {
    return tx.inTx(() -> revokeAll(sessions, outbox, userId, exceptDeviceId));
  }

  /** Inside a transaction: revoke sessions and queue one device revocation each. */
  static List<String> revokeAll(SessionStore sessions, Outbox outbox, String userId, String exceptDeviceId) {
    List<String> ids = sessions.revokeAll(userId, exceptDeviceId);
    ids.forEach(d -> outbox.revokeDevice(userId, d));
    return ids;
  }
}
