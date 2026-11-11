package com.pangate.auth.application.port;

import com.pangate.auth.domain.Client;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface ClientStore {

  /** @throws com.pangate.auth.domain.DomainException {@code client_exists} (409) on a duplicate */
  void insert(Client c);

  Optional<Client> find(String id);

  List<Client> listByTenant(String tenantId);

  /** Updates hashes, scopes and status. */
  void update(Client c);

  Set<String> disabledIds();
}
