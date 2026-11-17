package com.pangate.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.account.support.AccountIT;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

@AccountIT
class SchemaIT {

  @Autowired JdbcClient jdbc;

  @Test
  void migrationCreatesTheSpecTables() {
    assertThat(jdbc.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'account'")
        .query(String.class).list()).contains("accounts", "identifiers", "sessions", "admin_actions", "outbox");
    assertThat(jdbc.sql("SELECT udt_name FROM information_schema.columns WHERE table_schema = 'account' "
        + "AND table_name = 'accounts' AND column_name = 'username'").query(String.class).single())
        .isEqualTo("citext");
    jdbc.sql("INSERT INTO account.accounts (user_id, username, status) VALUES ('U1', 'Lan', 'active')").update();
    assertThatThrownBy(() -> jdbc.sql("INSERT INTO account.accounts (user_id, username, status) "
        + "VALUES ('U2', 'lan', 'active')").update()).isInstanceOf(DataIntegrityViolationException.class);
    jdbc.sql("INSERT INTO account.identifiers (kind, value, user_id) VALUES ('email', 'x@y.vn', 'U1')").update();
    assertThatThrownBy(() -> jdbc.sql("INSERT INTO account.identifiers (kind, value, user_id) "
        + "VALUES ('email', 'x@y.vn', 'U1')").update()).isInstanceOf(DataIntegrityViolationException.class);
  }
}
