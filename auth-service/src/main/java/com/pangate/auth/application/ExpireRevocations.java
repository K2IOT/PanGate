package com.pangate.auth.application;

import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.Outbox;
import com.pangate.auth.application.port.RevocationStore;
import com.pangate.auth.application.port.Transactions;
import com.pangate.auth.domain.Revocation;

/** Expired revocations become gw.control tombstones and are deleted, in one transaction (spec §5). */
public final class ExpireRevocations {

  static final int BATCH = 500;
  static final String TOPIC = "gw.control";

  private final RevocationStore store;
  private final Outbox outbox;
  private final Transactions tx;
  private final Clock clock;
  private final String topic;

  public ExpireRevocations(RevocationStore store, Outbox outbox, Transactions tx, Clock clock) {
    this(store, outbox, tx, clock, TOPIC);
  }

  public ExpireRevocations(RevocationStore store, Outbox outbox, Transactions tx, Clock clock, String topic) {
    this.store = store;
    this.outbox = outbox;
    this.tx = tx;
    this.clock = clock;
    this.topic = topic;
  }

  static final long BUDGET_NANOS = java.time.Duration.ofSeconds(20).toNanos();

  /**
   * Batches of {@value #BATCH}, one transaction each, until the backlog is drained or the pass has run for 20 s, so
   * expiry keeps up with the revocation intake (spec §7: 100/s per client). @return revocations tombstoned
   */
  public int runOnce() {
    long start = System.nanoTime();
    int total = 0;
    int batch;
    do {
      int[] n = {0};
      tx.inTx(() -> {
        for (Revocation r : store.expired(clock.now(), BATCH)) {
          outbox.append(topic, r.controlKey(), null, r.tenantId());
          store.delete(r.id());
          n[0]++;
        }
      });
      batch = n[0];
      total += batch;
    } while (batch == BATCH && System.nanoTime() - start < BUDGET_NANOS);
    return total;
  }
}
