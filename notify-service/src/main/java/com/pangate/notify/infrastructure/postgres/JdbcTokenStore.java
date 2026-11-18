package com.pangate.notify.infrastructure.postgres;

import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.domain.PushToken;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

/** {@code notify.push_tokens} (offline notifications spec §5.2). */
public final class JdbcTokenStore implements TokenStore {

  private static final String COLUMNS = "tenant_id, principal_id, device_id, platform, environment, token, updated_at";

  private final JdbcClient jdbc;
  private final TransactionTemplate tx;

  public JdbcTokenStore(JdbcClient jdbc, TransactionTemplate tx) {
    this.jdbc = jdbc;
    this.tx = tx;
  }

  static PushToken row(ResultSet rs, int n) throws SQLException {
    return new PushToken(rs.getString(1), rs.getString(2), rs.getString(3), Platform.parse(rs.getString(4)),
        rs.getString(5), rs.getString(6), rs.getTimestamp(7).toInstant());
  }

  @Override
  public int register(PushToken t) {
    // CH4 deferred minor: two devices registering one token at once both pass the DELETE, and the second INSERT
    // hits the unique (platform, token) index; trying again moves the token to the later one instead of a 500
    for (int attempt = 1; ; attempt++) {
      try {
        return registerOnce(t);
      } catch (org.springframework.dao.DuplicateKeyException e) {
        if (attempt == 3) {
          throw e;
        }
      }
    }
  }

  private int registerOnce(PushToken t) {
    return Db.call(() -> tx.execute(s -> {
      int moved = jdbc.sql("DELETE FROM push_tokens WHERE platform = ? AND token = ? AND NOT (tenant_id = ? AND "
              + "principal_id = ? AND device_id = ?)")
          .params(t.platform().wire(), t.token(), t.tenantId(), t.principalId(), t.deviceId()).update();
      jdbc.sql("INSERT INTO push_tokens (" + COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?) "
              + "ON CONFLICT (tenant_id, principal_id, device_id) DO UPDATE SET platform = EXCLUDED.platform, "
              + "environment = EXCLUDED.environment, token = EXCLUDED.token, updated_at = EXCLUDED.updated_at")
          .params(t.tenantId(), t.principalId(), t.deviceId(), t.platform().wire(), t.environment(), t.token(),
              Timestamp.from(t.updatedAt()))
          .update();
      return moved;
    }));
  }

  @Override
  public void remove(String tenant, String principal, String device) {
    Db.call(() -> jdbc.sql("DELETE FROM push_tokens WHERE tenant_id = ? AND principal_id = ? AND device_id = ?")
        .params(tenant, principal, device).update());
  }

  @Override
  public List<PushToken> byPrincipal(String tenant, String principal) {
    return Db.call(() -> jdbc.sql("SELECT " + COLUMNS + " FROM push_tokens WHERE tenant_id = ? AND principal_id = ?")
        .params(tenant, principal).query(JdbcTokenStore::row).list());
  }

  @Override
  public Optional<PushToken> byDevice(String tenant, String principal, String device) {
    return Db.call(() -> jdbc.sql("SELECT " + COLUMNS + " FROM push_tokens WHERE tenant_id = ? AND principal_id = ? "
            + "AND device_id = ?")
        .params(tenant, principal, device).query(JdbcTokenStore::row).optional());
  }

  @Override
  public int removeDevice(String principal, String device) {
    return Db.call(() -> jdbc.sql("DELETE FROM push_tokens WHERE principal_id = ? AND device_id = ?")
        .params(principal, device).update());
  }

  @Override
  public int removePrincipal(String principal) {
    return Db.call(() -> jdbc.sql("DELETE FROM push_tokens WHERE principal_id = ?").param(principal).update());
  }

  @Override
  public boolean removeIfToken(String tenant, String principal, String device, String token) {
    return Db.call(() -> jdbc.sql("DELETE FROM push_tokens WHERE tenant_id = ? AND principal_id = ? AND device_id = ? "
            + "AND token = ?")
        .params(tenant, principal, device, token).update()) > 0;
  }

  @Override
  public int purgeIdle(Instant before) {
    return Db.call(() -> jdbc.sql("DELETE FROM push_tokens WHERE updated_at < ?").param(Timestamp.from(before))
        .update());
  }
}
