package com.pangate.auth.application.port;

import java.time.Instant;
import java.util.List;

/** Transactional outbox (spec §4.4, §5): rows written with the business change, sent to Kafka afterwards. */
public interface Outbox {

  record OutboxRow(long id, String topic, String key, byte[] payload, String tenantId, Instant createdAt) {}

  /** {@code payloadOrNull == null} is a tombstone. */
  void append(String topic, String key, byte[] payloadOrNull, String tenantId);

  /** Oldest unsent rows by id, locked for this transaction ({@code FOR UPDATE SKIP LOCKED}). */
  List<OutboxRow> unsent(int limit);

  void markSent(long id);

  int deleteSentBefore(Instant t);
}
