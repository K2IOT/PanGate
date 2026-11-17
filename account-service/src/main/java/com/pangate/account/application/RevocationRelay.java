package com.pangate.account.application;

import com.pangate.account.application.port.AuthApi;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.application.port.Outbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends {@code revocation} outbox rows to auth-service in id order (account spec §6). A transient failure stops the
 * pass (the next pass resends from that row); a permanent one is logged, counted and marked sent, so it never blocks
 * the queue.
 */
public final class RevocationRelay {

  private static final Logger log = LoggerFactory.getLogger(RevocationRelay.class);
  private volatile long nextWarnAt;

  private final Outbox outbox;
  private final AuthApi auth;
  private final int batch;
  private final Metrics metrics;

  public RevocationRelay(Outbox outbox, AuthApi auth, int batch, Metrics metrics) {
    this.outbox = outbox;
    this.auth = auth;
    this.batch = batch;
    this.metrics = metrics;
  }

  /** Rows handled in this pass. */
  public int runOnce() {
    int done = 0;
    for (Outbox.OutboxRow r : outbox.pending(Outbox.REVOCATION, batch)) {
      try {
        auth.revoke(r.payload());
      } catch (AuthApi.AuthUnavailableException e) {
        metrics.count("account.revocations.unavailable");
        long now = System.currentTimeMillis();
        if (now >= nextWarnAt) {   // at most once a minute: a stall (rotated secret, disabled client) must be seen
          nextWarnAt = now + 60_000;
          log.warn("revocations stalled: auth-service unavailable ({}); {} kept for retry", e.getMessage(), r.id());
        }
        return done;
      } catch (AuthApi.AuthRejectedException e) {
        log.error("revocation {} refused by auth-service ({}); dropped", r.id(), e.status());
        metrics.count("account.revocations.rejected");
      }
      outbox.markSent(r.id());
      done++;
    }
    return done;
  }
}
