package com.pangate.account.application;

import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.SessionStore;
import java.time.Duration;

/**
 * Hourly (account spec §6): sessions unused for {@code idleTtl} are revoked (their connect tokens expired long ago,
 * so no revocation is sent); revoked sessions older than 30 days and sent outbox rows older than {@code keepSent} are
 * deleted.
 */
public final class ExpireSessions {

  static final Duration KEEP_REVOKED = Duration.ofDays(30);

  private final SessionStore sessions;
  private final Outbox outbox;
  private final Clock clock;
  private final Duration idleTtl;
  private final Duration keepSent;

  public ExpireSessions(SessionStore sessions, Outbox outbox, Clock clock, Duration idleTtl, Duration keepSent) {
    this.sessions = sessions;
    this.outbox = outbox;
    this.clock = clock;
    this.idleTtl = idleTtl;
    this.keepSent = keepSent;
  }

  public void runOnce() {
    sessions.expireIdle(clock.now().minus(idleTtl));
    sessions.purgeRevoked(clock.now().minus(KEEP_REVOKED));
    outbox.purgeSent(clock.now().minus(keepSent));
  }
}
