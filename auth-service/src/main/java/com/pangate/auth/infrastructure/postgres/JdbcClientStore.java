package com.pangate.auth.infrastructure.postgres;

import com.pangate.auth.application.port.ClientStore;
import com.pangate.auth.domain.Client;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Scope;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcClientStore implements ClientStore {

  private static final String COLS = "id, tenant_id, secret_hash, secret_hash_next, array_to_string(scopes, ' '), status";

  private final JdbcClient jdbc;

  public JdbcClientStore(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  private static Client row(ResultSet rs, int i) throws SQLException {
    return new Client(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), Scope.parse(rs.getString(5)),
        "active".equals(rs.getString(6)));
  }

  /** Postgres array literal; scope values contain no characters that need quoting. */
  private static String array(Set<Scope> scopes) {
    return scopes.stream().sorted().map(Scope::value).collect(Collectors.joining(",", "{", "}"));  // enum order
  }

  @Override
  public void insert(Client c) {
    try {
      jdbc.sql("INSERT INTO auth.clients (id, tenant_id, secret_hash, secret_hash_next, scopes, status)"
          + " VALUES (:id, :t, :h, :n, CAST(:s AS text[]), :st)")
          .param("id", c.id()).param("t", c.tenantId()).param("h", c.secretHash()).param("n", c.secretHashNext())
          .param("s", array(c.scopes())).param("st", c.active() ? "active" : "disabled").update();
    } catch (DuplicateKeyException e) {
      throw new DomainException("client_exists", 409, "client " + c.id() + " exists");
    }
  }

  @Override
  public Optional<Client> find(String id) {
    return jdbc.sql("SELECT " + COLS + " FROM auth.clients WHERE id = :id").param("id", id)
        .query(JdbcClientStore::row).optional();
  }

  @Override
  public List<Client> listByTenant(String tenantId) {
    return jdbc.sql("SELECT " + COLS + " FROM auth.clients WHERE tenant_id = :t ORDER BY id").param("t", tenantId)
        .query(JdbcClientStore::row).list();
  }

  @Override
  public void update(Client c) {
    jdbc.sql("UPDATE auth.clients SET secret_hash = :h, secret_hash_next = :n, scopes = CAST(:s AS text[]),"
        + " status = :st, secret_rotated_at = CASE WHEN secret_hash IS DISTINCT FROM :h"
        + " OR secret_hash_next IS DISTINCT FROM :n THEN now() ELSE secret_rotated_at END WHERE id = :id")
        .param("h", c.secretHash()).param("n", c.secretHashNext()).param("s", array(c.scopes()))
        .param("st", c.active() ? "active" : "disabled").param("id", c.id()).update();
  }

  @Override
  public Set<String> disabledIds() {
    return new HashSet<>(jdbc.sql("SELECT id FROM auth.clients WHERE status = 'disabled'").query(String.class).list());
  }
}
