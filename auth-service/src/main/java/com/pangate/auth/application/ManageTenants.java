package com.pangate.auth.application;

import com.pangate.auth.application.port.TenantStore;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Tenant;
import java.util.List;
import java.util.regex.Pattern;

/** Admin use cases on tenants (spec §4.5). */
public final class ManageTenants {

  static final Pattern ID = Pattern.compile("^[a-z0-9][a-z0-9_-]{0,63}$");

  private final TenantStore tenants;

  public ManageTenants(TenantStore tenants) {
    this.tenants = tenants;
  }

  public Tenant create(String id) {
    if (id == null || !ID.matcher(id).matches() || Tenant.WARMUP.equals(id)) {
      throw DomainException.badRequest("tenant_invalid", "tenant id must match " + ID.pattern());
    }
    Tenant t = new Tenant(id, true);
    tenants.insert(t);
    return t;
  }

  /** Creates the tenant unless it exists; used by the dev seed. */
  public void ensure(String id) {
    if (tenants.find(id).isEmpty()) {
      create(id);
    }
  }

  public List<Tenant> list() {
    return tenants.list();
  }

  public Tenant get(String id) {
    return tenants.find(id).orElseThrow(() -> notFound(id));
  }

  public Tenant disable(String id) {
    return set(id, false);
  }

  public Tenant enable(String id) {
    return set(id, true);
  }

  private Tenant set(String id, boolean active) {
    if (!tenants.setActive(id, active)) {
      throw notFound(id);
    }
    return new Tenant(id, active);
  }

  static DomainException notFound(String id) {
    return new DomainException("tenant_not_found", 404, "no tenant " + id);
  }
}
