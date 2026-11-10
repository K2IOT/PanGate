package com.pangate.auth.application.port;

import com.pangate.auth.domain.Tenant;
import java.util.List;
import java.util.Optional;

public interface TenantStore {

  /** @throws com.pangate.auth.domain.DomainException {@code tenant_exists} (409) on a duplicate */
  void insert(Tenant t);

  Optional<Tenant> find(String id);

  List<Tenant> list();

  /** @return false if the tenant does not exist */
  boolean setActive(String id, boolean active);
}
