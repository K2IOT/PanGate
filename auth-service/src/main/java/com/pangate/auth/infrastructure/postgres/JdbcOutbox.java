package com.pangate.auth.infrastructure.postgres;

import com.pangate.auth.application.port.Outbox;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcOutbox implements Outbox {

  private final JdbcClient jdbc;

  public JdbcOutbox(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void append(String topic, String key, byte[] payloadOrNull, String tenantId) {
    jdbc.sql("INSERT INTO auth.outbox (topic, record_key, payload, tenant_id) VALUES (:t, :k, :p, :tenant)")
        .param("t", topic).param("k", key).param("p", payloadOrNull, java.sql.Types.BINARY)
        .param("tenant", tenantId == null ? "" : tenantId).update();
  }

  @Override
  public List<OutboxRow> unsent(int limit) {
    return jdbc.sql("SELECT id, topic, record_key, payload, tenant_id, created_at FROM auth.outbox"
        + " WHERE sent_at IS NULL ORDER BY id LIMIT :n FOR UPDATE SKIP LOCKED").param("n", limit)
        .query((rs, i) -> new OutboxRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBytes(4),
            rs.getString(5), rs.getTimestamp(6).toInstant())).list();
  }

  @Override
  public void markSent(long id) {
    jdbc.sql("UPDATE auth.outbox SET sent_at = now() WHERE id = :id").param("id", id).update();
  }

  @Override
  public int deleteSentBefore(Instant t) {
    return jdbc.sql("DELETE FROM auth.outbox WHERE sent_at IS NOT NULL AND sent_at < :t")
        .param("t", Timestamp.from(t)).update();
  }

  /** Unsent rows and the age of the oldest one (metrics, spec §7). */
  public record Backlog(long pending, Optional<Instant> oldest) {}

  public Backlog backlog() {
    return jdbc.sql("SELECT count(*), min(created_at) FROM auth.outbox WHERE sent_at IS NULL")
        .query((rs, i) -> new Backlog(rs.getLong(1), Optional.ofNullable(rs.getTimestamp(2))
            .map(Timestamp::toInstant))).single();
  }
}
