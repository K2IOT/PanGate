package com.pangate.auth.application;

import com.pangate.auth.application.port.ClientStore;
import com.pangate.auth.application.port.TenantStore;
import com.pangate.auth.domain.Client;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Tenant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

final class InMemoryStores {

  static final class Tenants implements TenantStore {
    final Map<String, Tenant> rows = new TreeMap<>();

    @Override
    public void insert(Tenant t) {
      if (rows.putIfAbsent(t.id(), t) != null) {
        throw new DomainException("tenant_exists", 409);
      }
    }

    @Override
    public Optional<Tenant> find(String id) {
      return Optional.ofNullable(rows.get(id));
    }

    @Override
    public List<Tenant> list() {
      return List.copyOf(rows.values());
    }

    @Override
    public boolean setActive(String id, boolean active) {
      return rows.computeIfPresent(id, (k, t) -> new Tenant(k, active)) != null;
    }
  }

  static final class Clients implements ClientStore {
    final Map<String, Client> rows = new TreeMap<>();

    @Override
    public void insert(Client c) {
      if (rows.putIfAbsent(c.id(), c) != null) {
        throw new DomainException("client_exists", 409);
      }
    }

    @Override
    public Optional<Client> find(String id) {
      return Optional.ofNullable(rows.get(id));
    }

    @Override
    public List<Client> listByTenant(String tenantId) {
      return rows.values().stream().filter(c -> c.tenantId().equals(tenantId)).toList();
    }

    @Override
    public void update(Client c) {
      rows.put(c.id(), c);
    }

    @Override
    public Set<String> disabledIds() {
      return rows.values().stream().filter(c -> !c.active()).map(Client::id).collect(Collectors.toSet());
    }
  }

  private InMemoryStores() {}
}
