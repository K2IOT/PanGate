package com.pangate.auth.infrastructure.postgres;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Runs a task inside a transaction holding {@code pg_try_advisory_xact_lock(hashtext(name))}, so only one pod runs a
 * job pass at a time (spec §5). The lock is released when the transaction ends.
 */
public class AdvisoryLock {

  private final TransactionTemplate tx;
  private final JdbcClient jdbc;

  public AdvisoryLock(TransactionTemplate tx, JdbcClient jdbc) {
    this.tx = tx;
    this.jdbc = jdbc;
  }

  /** @return false if another session holds the lock (the task did not run) */
  public boolean runExclusive(String name, Runnable task) {
    Boolean ran = tx.execute(status -> {
      boolean got = Boolean.TRUE.equals(jdbc.sql("SELECT pg_try_advisory_xact_lock(hashtext(:n))").param("n", name)
          .query(Boolean.class).single());
      if (got) {
        task.run();
      }
      return got;
    });
    return Boolean.TRUE.equals(ran);
  }
}
