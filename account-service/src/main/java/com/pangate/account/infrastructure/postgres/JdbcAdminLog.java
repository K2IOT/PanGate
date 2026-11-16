package com.pangate.account.infrastructure.postgres;

import com.pangate.account.application.port.AdminLog;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;

/** admin_actions (account spec §5). */
public final class JdbcAdminLog implements AdminLog {

  private final JdbcClient jdbc;

  public JdbcAdminLog(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void record(String actor, String action, String userId, String reason) {
    Db.run(() -> jdbc.sql("INSERT INTO account.admin_actions (actor, action, user_id, reason) VALUES (?, ?, ?, ?)")
        .params(actor, action, userId, reason).update());
  }

  @Override
  public List<Entry> of(String userId) {
    return Db.call(() -> jdbc.sql("SELECT actor, action, reason, at FROM account.admin_actions WHERE user_id = ? "
        + "ORDER BY id").param(userId).query((rs, n) -> new Entry(rs.getString("actor"), rs.getString("action"),
        rs.getString("reason"), rs.getTimestamp("at").toInstant())).list());
  }
}
