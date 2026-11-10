package com.pangate.auth.api;

import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Scope;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Set;

/** The authenticated API caller, set by {@link ApiAuthFilter}. */
public record ApiPrincipal(String clientId, String tenant, Set<Scope> scopes, String jti) {

  public static final String ATTR = ApiPrincipal.class.getName();

  public static ApiPrincipal of(HttpServletRequest req) {
    Object p = req.getAttribute(ATTR);
    if (!(p instanceof ApiPrincipal ap)) {
      throw new DomainException("invalid_token", 401, "authentication required");
    }
    return ap;
  }

  /** @throws DomainException {@code missing_scope} (403) */
  public ApiPrincipal require(Scope scope) {
    if (!scopes.contains(scope)) {
      throw new DomainException("missing_scope", 403, "scope " + scope.value() + " required");
    }
    return this;
  }
}
