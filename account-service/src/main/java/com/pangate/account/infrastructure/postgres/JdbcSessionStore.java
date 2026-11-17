package com.pangate.account.infrastructure.postgres;

import com.pangate.account.application.port.SessionStore;
import com.pangate.account.domain.Session;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;

/** sessions (account spec §2, §3). */
public final class JdbcSessionStore implements SessionStore {

  private static final String COLS = "device_id, user_id, device_name, platform, created_at, last_used_at, revoked_at";

  private final JdbcClient jdbc;

  public JdbcSessionStore(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  static Session map(ResultSet rs, int row) throws SQLException {
    Timestamp revoked = rs.getTimestamp("revoked_at");
    return new Session(rs.getString("device_id"), rs.getString("user_id"), rs.getString("device_name"),
        rs.getString("platform"), rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("last_used_at").toInstant(), revoked == null ? null : revoked.toInstant());
  }

  @Override
  public void insert(Session s, byte[] refreshHash) {
    Db.run(() -> jdbc.sql("INSERT INTO account.sessions (device_id, user_id, refresh_hash, device_name, platform) "
        + "VALUES (?, ?, ?, ?, ?)").params(s.deviceId(), s.userId(), refreshHash, s.deviceName(), s.platform())
        .update());
  }

  @Override
  public Optional<Session> byRefreshHash(byte[] hash) {
    return Db.call(() -> jdbc.sql("SELECT " + COLS + " FROM account.sessions WHERE refresh_hash = ?").param(hash)
        .query(JdbcSessionStore::map).optional());
  }

  @Override
  public Optional<Session> byDeviceId(String deviceId) {
    return Db.call(() -> jdbc.sql("SELECT " + COLS + " FROM account.sessions WHERE device_id = ?").param(deviceId)
        .query(JdbcSessionStore::map).optional());
  }

  @Override
  public List<Session> live(String userId) {
    return Db.call(() -> jdbc.sql("SELECT " + COLS + " FROM account.sessions WHERE user_id = ? AND revoked_at IS NULL "
        + "ORDER BY created_at, device_id").param(userId).query(JdbcSessionStore::map).list());
  }

  @Override
  public List<String> revokeAll(String userId, String exceptDeviceId) {
    return Db.call(() -> jdbc.sql("UPDATE account.sessions SET revoked_at = now() WHERE user_id = ? "
            + "AND revoked_at IS NULL AND device_id IS DISTINCT FROM ? RETURNING device_id")
        .params(userId, exceptDeviceId).query(String.class).list());
  }

  @Override
  public boolean revoke(String userId, String deviceId) {
    return Db.call(() -> jdbc.sql("UPDATE account.sessions SET revoked_at = now() WHERE user_id = ? AND device_id = ? "
        + "AND revoked_at IS NULL").params(userId, deviceId).update() == 1);
  }

  @Override
  public void touch(String deviceId, Instant at) {
    Db.run(() -> jdbc.sql("UPDATE account.sessions SET last_used_at = ? WHERE device_id = ?")
        .params(Timestamp.from(at), deviceId).update());
  }

  @Override
  public boolean isLive(String userId, String deviceId) {
    return Db.call(() -> jdbc.sql("SELECT count(*) FROM account.sessions WHERE device_id = ? AND user_id = ? "
        + "AND revoked_at IS NULL").params(deviceId, userId).query(Long.class).single() == 1);
  }

  @Override
  public int expireIdle(Instant before) {
    return Db.call(() -> jdbc.sql("UPDATE account.sessions SET revoked_at = now() WHERE revoked_at IS NULL "
        + "AND last_used_at < ?").param(Timestamp.from(before)).update());
  }

  @Override
  public int purgeRevoked(Instant before) {
    return Db.call(() -> jdbc.sql("DELETE FROM account.sessions WHERE revoked_at < ?").param(Timestamp.from(before))
        .update());
  }

  @Override
  public long countLive() {
    return Db.call(() -> jdbc.sql("SELECT count(*) FROM account.sessions WHERE revoked_at IS NULL").query(Long.class)
        .single());
  }
}
