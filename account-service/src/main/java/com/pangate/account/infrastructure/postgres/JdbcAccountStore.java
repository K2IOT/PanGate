package com.pangate.account.infrastructure.postgres;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.TakenException;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.Login;
import com.pangate.account.domain.LoginKind;
import com.pangate.account.domain.Username;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

/** accounts + identifiers in schema {@code account} (account spec §3). */
public final class JdbcAccountStore implements AccountStore {

  private static final String SELECT = "SELECT a.user_id, a.username::text AS username, a.display_name, a.bio, "
      + "a.password_hash, a.must_change_password, a.status, a.created_at, a.deleted_at, e.value AS email, "
      + "p.value AS phone FROM account.accounts a "
      + "LEFT JOIN account.identifiers e ON e.user_id = a.user_id AND e.kind = 'email' "
      + "LEFT JOIN account.identifiers p ON p.user_id = a.user_id AND p.kind = 'phone' ";

  private final JdbcClient jdbc;

  public JdbcAccountStore(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  static Account map(ResultSet rs, int row) throws SQLException {
    Timestamp deleted = rs.getTimestamp("deleted_at");
    return new Account(rs.getString("user_id"), rs.getString("username"), rs.getString("display_name"),
        rs.getString("bio"), rs.getString("password_hash"), rs.getBoolean("must_change_password"),
        AccountStatus.valueOf(rs.getString("status").toUpperCase(Locale.ROOT)), rs.getString("email"),
        rs.getString("phone"), rs.getTimestamp("created_at").toInstant(), deleted == null ? null : deleted.toInstant());
  }

  static String kind(LoginKind k) {
    return k == LoginKind.EMAIL ? "email" : "phone";
  }

  @Override
  public void insert(Account a) {
    Db.run(() -> {
      try {
        jdbc.sql("INSERT INTO account.accounts (user_id, username, display_name, bio, password_hash, "
                + "must_change_password, status) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .params(a.userId(), a.username(), a.displayName(), a.bio(), a.passwordHash(), a.mustChangePassword(),
                a.status().name().toLowerCase(Locale.ROOT)).update();
      } catch (DuplicateKeyException e) {
        throw new TakenException("username_taken");
      }
      if (a.email() != null) {
        insertIdentifier(a.userId(), LoginKind.EMAIL, a.email());
      }
      if (a.phone() != null) {
        insertIdentifier(a.userId(), LoginKind.PHONE, a.phone());
      }
    });
  }

  private void insertIdentifier(String userId, LoginKind kind, String value) {
    try {
      jdbc.sql("INSERT INTO account.identifiers (kind, value, user_id) VALUES (?, ?, ?)")
          .params(kind(kind), value, userId).update();
    } catch (DuplicateKeyException e) {
      throw new TakenException(kind == LoginKind.EMAIL ? "email_taken" : "phone_taken");
    }
  }

  @Override
  public Optional<Account> byId(String userId) {
    return Db.call(() -> jdbc.sql(SELECT + "WHERE a.user_id = ?").param(userId).query(JdbcAccountStore::map)
        .optional());
  }

  @Override
  public Optional<Account> byLogin(Login login) {
    return Db.call(() -> switch (login.kind()) {
      case USERNAME -> jdbc.sql(SELECT + "WHERE a.username = ?::account.citext").param(login.key()).query(JdbcAccountStore::map)
          .optional();
      case EMAIL, PHONE -> jdbc.sql(SELECT + "WHERE a.user_id = (SELECT user_id FROM account.identifiers "
              + "WHERE kind = ? AND value = ?)").params(kind(login.kind()), login.key()).query(JdbcAccountStore::map)
          .optional();
    });
  }

  @Override
  public Optional<Account> byUsernameKey(String key) {
    return Db.call(() -> jdbc.sql(SELECT + "WHERE a.username = ?::account.citext").param(key).query(JdbcAccountStore::map)
        .optional());
  }

  @Override
  public List<Account> byIds(Collection<String> userIds) {
    if (userIds.isEmpty()) {
      return List.of();
    }
    return Db.call(() -> jdbc.sql(SELECT + "WHERE a.user_id = ANY (?)")
        .param(userIds.toArray(String[]::new)).query(JdbcAccountStore::map).list());
  }

  @Override
  public void updateProfile(String userId, String displayName, String bio) {
    Db.run(() -> jdbc.sql("UPDATE account.accounts SET display_name = ?, bio = ?, updated_at = now() "
        + "WHERE user_id = ?").params(displayName, bio, userId).update());
  }

  @Override
  public void updateUsername(String userId, Username username) {
    Db.run(() -> {
      try {
        jdbc.sql("UPDATE account.accounts SET username = ?, updated_at = now() WHERE user_id = ?")
            .params(username.display(), userId).update();
      } catch (DuplicateKeyException e) {
        throw new TakenException("username_taken");
      }
    });
  }

  @Override
  public void setIdentifier(String userId, LoginKind kind, String value) {
    Db.run(() -> {
      jdbc.sql("DELETE FROM account.identifiers WHERE user_id = ? AND kind = ?").params(userId, kind(kind)).update();
      if (value != null) {
        insertIdentifier(userId, kind, value);
      }
    });
  }

  @Override
  public void setPassword(String userId, String hash, boolean mustChange) {
    Db.run(() -> jdbc.sql("UPDATE account.accounts SET password_hash = ?, must_change_password = ?, "
        + "password_changed_at = now(), updated_at = now() WHERE user_id = ?").params(hash, mustChange, userId)
        .update());
  }

  @Override
  public void setStatus(String userId, AccountStatus status) {
    Db.run(() -> jdbc.sql("UPDATE account.accounts SET status = ?, updated_at = now() WHERE user_id = ?")
        .params(status.name().toLowerCase(Locale.ROOT), userId).update());
  }

  @Override
  public void erase(String userId) {
    Db.run(() -> {
      jdbc.sql("DELETE FROM account.identifiers WHERE user_id = ?").param(userId).update();
      jdbc.sql("UPDATE account.accounts SET username = NULL, display_name = NULL, bio = NULL, password_hash = NULL, "
          + "must_change_password = false, status = 'deleted', deleted_at = now(), updated_at = now() "
          + "WHERE user_id = ?").param(userId).update();
    });
  }

  static Instant now() {
    return Instant.now();
  }
}
