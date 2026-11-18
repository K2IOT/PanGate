package com.pangate.account.infrastructure.postgres;

import com.pangate.account.application.port.Outbox;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.AccountState;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;

/** outbox (account spec §6). Revocation payloads are the JSON body of auth-service {@code POST /v1/revocations}. */
public final class JdbcOutbox implements Outbox {

  private final JdbcClient jdbc;

  public JdbcOutbox(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  private void add(String kind, String key, byte[] payload) {
    Db.run(() -> jdbc.sql("INSERT INTO account.outbox (kind, record_key, payload) VALUES (?, ?, ?)")
        .params(kind, key, payload).update());
  }

  static String principal(String userId) {
    return ChatTopics.PRINCIPAL_PREFIX + userId;
  }

  @Override
  public void event(String userId, AccountState.Status status) {
    long at = Db.call(() -> jdbc.sql("SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint")
        .query(Long.class).single());
    // offline notifications spec §2.2: the name as of this transaction; none once deleted
    String name = status == AccountState.Status.STATUS_DELETED ? null : Db.call(() -> jdbc.sql(
            "SELECT display_name FROM accounts WHERE user_id = ?").param(userId).query(String.class).optional())
        .orElse(null);
    add(EVENT, userId, AccountState.newBuilder().setUserId(userId).setStatus(status).setUpdatedAtMs(at)
        .setDisplayName(name == null ? "" : name).build().toByteArray());
  }

  @Override
  public void revokeDevice(String userId, String deviceId) {
    add(REVOCATION, userId, ("{\"kind\":\"device\",\"principal\":\"" + principal(userId) + "\",\"device\":\""
        + deviceId + "\"}").getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void revokePrincipal(String userId) {
    add(REVOCATION, userId, ("{\"kind\":\"principal\",\"principal\":\"" + principal(userId) + "\"}")
        .getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public List<OutboxRow> pending(String kind, int limit) {
    return Db.call(() -> jdbc.sql("SELECT id, kind, record_key, payload FROM account.outbox WHERE kind = ? "
            + "AND sent_at IS NULL ORDER BY id LIMIT ?").params(kind, limit)
        .query((rs, n) -> new OutboxRow(rs.getLong("id"), rs.getString("kind"), rs.getString("record_key"),
            rs.getBytes("payload"))).list());
  }

  @Override
  public void markSent(long id) {
    Db.run(() -> jdbc.sql("UPDATE account.outbox SET sent_at = now() WHERE id = ?").param(id).update());
  }

  @Override
  public int purgeSent(Instant before) {
    return Db.call(() -> jdbc.sql("DELETE FROM account.outbox WHERE sent_at < ?").param(Timestamp.from(before))
        .update());
  }

  @Override
  public long pendingCount(String kind) {
    return Db.call(() -> jdbc.sql("SELECT count(*) FROM account.outbox WHERE kind = ? AND sent_at IS NULL")
        .param(kind).query(Long.class).single());
  }

  @Override
  public double oldestAgeSeconds(String kind) {
    return Db.call(() -> jdbc.sql("SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) "
        + "FROM account.outbox WHERE kind = ? AND sent_at IS NULL").param(kind).query(Double.class).single());
  }
}
