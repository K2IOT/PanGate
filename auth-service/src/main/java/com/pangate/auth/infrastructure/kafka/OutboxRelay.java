package com.pangate.auth.infrastructure.kafka;

import com.pangate.auth.application.port.Outbox;
import com.pangate.auth.application.port.Outbox.OutboxRow;
import com.pangate.auth.infrastructure.postgres.AdvisoryLock;
import com.pangate.starter.kafka.PangateHeaders;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends unsent outbox rows in id order, one acknowledged record at a time; stops at the first failure so the next
 * pass resends from the first unsent row (spec §5). One pod at a time via lock {@code auth.outbox-relay}.
 */
public final class OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
  static final String LOCK = "auth.outbox-relay";
  static final long SEND_TIMEOUT_MS = 15_000;

  private final Outbox outbox;
  private final AdvisoryLock lock;
  private final Supplier<Producer<String, byte[]>> producer;
  private final int batch;
  private final String cell;
  private volatile boolean lastFailed;

  public OutboxRelay(Outbox outbox, AdvisoryLock lock, Supplier<Producer<String, byte[]>> producer, int batch,
      String cell) {
    this.outbox = outbox;
    this.lock = lock;
    this.producer = producer;
    this.batch = batch;
    this.cell = cell == null ? "" : cell;
  }

  /** @return records sent in this pass */
  public int runOnce() {
    int[] sent = {0};
    lock.runExclusive(LOCK, () -> {
      for (OutboxRow row : outbox.unsent(batch)) {
        try {
          ProducerRecord<String, byte[]> rec = new ProducerRecord<>(row.topic(), row.key(), row.payload());
          PangateHeaders.write(rec.headers(), null, row.tenantId(), cell, "ControlEvent");
          producer.get().send(rec).get(SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          if (!lastFailed) {
            log.warn("outbox relay paused at row {}: {}", row.id(), e.toString());
          }
          lastFailed = true;
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          return;
        }
        outbox.markSent(row.id());
        sent[0]++;
      }
    });
    if (sent[0] > 0 && lastFailed) {
      log.info("outbox relay resumed");
      lastFailed = false;
    }
    return sent[0];
  }
}
