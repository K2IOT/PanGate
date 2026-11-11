package com.pangate.auth.domain;

import java.util.Set;

/** The authenticated API client behind a request (from its API token). */
public record Caller(String clientId, String tenant, Set<Scope> scopes, String jti) {

  public Caller {
    scopes = Set.copyOf(scopes);
  }

  /** @throws DomainException {@code missing_scope} (403) */
  public Caller require(Scope scope) {
    if (!scopes.contains(scope)) {
      throw new DomainException("missing_scope", 403, "scope " + scope.value() + " required");
    }
    return this;
  }
}
