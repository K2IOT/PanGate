package com.pangate.auth.infrastructure.postgres;

import com.pangate.auth.application.port.TenantStore;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Tenant;
import java.util.List;
import java.util.Optional;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcTenantStore implements TenantStore {

  private final JdbcClient jdbc;

  public JdbcTenantStore(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void insert(Tenant t) {
    try {
      jdbc.sql("INSERT INTO auth.tenants (id, status) VALUES (:id, :st)").param("id", t.id())
          .param("st", t.active() ? "active" : "disabled").update();
    } catch (DuplicateKeyException e) {
      throw new DomainException("tenant_exists", 409, "tenant " + t.id() + " exists");
    }
  }

  @Override
  public Optional<Tenant> find(String id) {
    return jdbc.sql("SELECT id, status FROM auth.tenants WHERE id = :id").param("id", id)
        .query((rs, i) -> new Tenant(rs.getString(1), "active".equals(rs.getString(2)))).optional();
  }

  @Override
  public List<Tenant> list() {
    return jdbc.sql("SELECT id, status FROM auth.tenants ORDER BY id")
        .query((rs, i) -> new Tenant(rs.getString(1), "active".equals(rs.getString(2)))).list();
  }

  @Override
  public boolean setActive(String id, boolean active) {
    return jdbc.sql("UPDATE auth.tenants SET status = :st WHERE id = :id").param("st", active ? "active" : "disabled")
        .param("id", id).update() == 1;
  }
}
