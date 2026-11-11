package com.pangate.auth.api;

import com.pangate.auth.domain.Caller;
import com.pangate.auth.domain.DomainException;
import jakarta.servlet.http.HttpServletRequest;

/** Where {@link ApiAuthFilter} leaves the {@link Caller} of a request. */
public final class ApiPrincipal {

  public static final String ATTR = Caller.class.getName();

  private ApiPrincipal() {}

  public static Caller of(HttpServletRequest req) {
    if (!(req.getAttribute(ATTR) instanceof Caller c)) {
      throw new DomainException("invalid_token", 401, "authentication required");
    }
    return c;
  }
}
