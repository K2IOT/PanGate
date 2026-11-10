package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.auth.support.AuthIT;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

@AuthIT
class SchemaIT {

  @Autowired JdbcClient jdbc;
  @Autowired TransactionTemplate tx;

  @Test
  void migrationCreatesTablesAndConstraints() {
    var tables = jdbc.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'auth'")
        .query(String.class).list();
    assertThat(tables).contains("tenants", "clients", "signing_keys", "revocations", "outbox");
    var indexes = jdbc.sql("SELECT indexdef FROM pg_indexes WHERE schemaname = 'auth' AND tablename = 'revocations'")
        .query(String.class).list();
    assertThat(indexes).anySatisfy(d -> assertThat(d).contains("(control_key)"));

    assertThatThrownBy(() -> jdbc.sql("INSERT INTO auth.tenants (id, status) VALUES ('__warmup', 'active')").update())
        .isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("check");

    String insertKey = "INSERT INTO auth.signing_keys (kid, private_key_enc, kek_id, public_jwk, state, activate_at)"
        + " VALUES (?, '\\x00'::bytea, 'v1', '{}'::jsonb, 'active', now())";
    // Inside a transaction that always rolls back, so keys of the running application are untouched.
    assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
      jdbc.sql("UPDATE auth.signing_keys SET state = 'retired' WHERE state = 'active'").update();
      jdbc.sql(insertKey).param("schema-it-1").update();
      jdbc.sql(insertKey).param("schema-it-2").update();
    })).isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("one_active_key");
  }
}
