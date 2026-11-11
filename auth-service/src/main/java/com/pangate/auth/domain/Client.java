package com.pangate.auth.domain;

import java.util.Set;

/** An API client of a tenant; {@code secretHashNext} is set while a secret rotation is in progress. */
public record Client(String id, String tenantId, String secretHash, String secretHashNext, Set<Scope> scopes,
    boolean active) {

  /** Only clients of the operator tenant may hold {@link Scope#ADMIN} (spec §4.1). */
  public static void checkScopes(String tenantId, Set<Scope> scopes) {
    if (scopes.contains(Scope.ADMIN) && !Tenant.isOperator(tenantId)) {
      throw new DomainException("scope_not_allowed", 400, "scope admin is reserved for the operator tenant");
    }
  }
}
