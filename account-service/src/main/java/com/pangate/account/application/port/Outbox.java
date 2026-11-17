package com.pangate.account.application.port;

import com.pangate.contracts.v1.AccountState;
import java.time.Instant;
import java.util.List;

/**
 * account spec §6: rows written in the same transaction as the change. {@code event} rows go to Kafka
 * {@code account.users}; {@code revocation} rows are POSTed to auth-service {@code /v1/revocations}.
 */
public interface Outbox {

  String EVENT = "event";
  String REVOCATION = "revocation";

  record OutboxRow(long id, String kind, String key, byte[] payload) {}

  /** {@code AccountState} of the user with {@code updated_at_ms} from the database clock: one clock for all pods. */
  void event(String userId, AccountState.Status status);

  void revokeDevice(String userId, String deviceId);

  void revokePrincipal(String userId);

  List<OutboxRow> pending(String kind, int limit);

  void markSent(long id);

  int purgeSent(Instant before);

  long pendingCount(String kind);

  /** Age in seconds of the oldest unsent row of that kind, 0 when none. */
  double oldestAgeSeconds(String kind);
}
