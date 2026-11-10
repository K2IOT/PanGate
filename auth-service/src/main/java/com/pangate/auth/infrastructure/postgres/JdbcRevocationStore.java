package com.pangate.auth.infrastructure.postgres;

import com.pangate.auth.application.port.RevocationStore;
import com.pangate.auth.domain.Revocation;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcRevocationStore implements RevocationStore {

  private final JdbcClient jdbc;

  public JdbcRevocationStore(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public boolean isJtiRevoked(String jti) {
    return Boolean.TRUE.equals(jdbc.sql("SELECT EXISTS (SELECT 1 FROM auth.revocations WHERE control_key = :k"
        + " AND expires_at > now())").param("k", "revoke:j:" + jti).query(Boolean.class).single());
  }

  /** Serialised per control key with a transaction-scoped advisory lock (control_key has no unique index). */
  @Override
  public Revocation upsert(Revocation r) {
    jdbc.sql("SELECT pg_advisory_xact_lock(hashtext(:k))").param("k", "auth.revocation:" + r.controlKey())
        .query((rs, i) -> 1).list();
    Timestamp exp = Timestamp.from(r.expiresAt().truncatedTo(ChronoUnit.MICROS));
    // Extending keeps the first actor (created_by) and returns the stored expiry, which is what gets published.
    org.springframework.jdbc.core.RowMapper<Revocation> row = (rs, i) -> new Revocation(rs.getLong(1), r.tenantId(),
        r.kind(), r.controlKey(), rs.getTimestamp(2).toInstant(), rs.getString(3));
    Optional<Revocation> extended = jdbc.sql("UPDATE auth.revocations SET expires_at = GREATEST(expires_at, :e)"
        + " WHERE control_key = :k RETURNING id, expires_at, created_by").param("e", exp).param("k", r.controlKey())
        .query(row).optional();
    return extended.orElseGet(() -> jdbc.sql("INSERT INTO auth.revocations (tenant_id, kind, control_key, expires_at,"
        + " created_by) VALUES (:t, :kind, :k, :e, :by) RETURNING id, expires_at, created_by").param("t", r.tenantId())
        .param("kind", r.kind().db()).param("k", r.controlKey()).param("e", exp).param("by", r.createdBy())
        .query(row).single());
  }

  @Override
  public List<Revocation> expired(Instant now, int limit) {
    return jdbc.sql("SELECT id, tenant_id, kind, control_key, expires_at, created_by FROM auth.revocations"
        + " WHERE expires_at <= :now ORDER BY id LIMIT :n FOR UPDATE SKIP LOCKED")
        .param("now", Timestamp.from(now)).param("n", limit)
        .query((rs, i) -> new Revocation(rs.getLong(1), rs.getString(2), Revocation.Kind.fromDb(rs.getString(3)),
            rs.getString(4), rs.getTimestamp(5).toInstant(), rs.getString(6))).list();
  }

  @Override
  public void delete(long id) {
    jdbc.sql("DELETE FROM auth.revocations WHERE id = :id").param("id", id).update();
  }
}
