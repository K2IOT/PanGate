package com.pangate.auth.api;

import com.pangate.auth.application.KeyRing;
import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.domain.ApiClaims;
import com.pangate.auth.domain.Caller;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.springframework.http.HttpHeaders;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Bearer API tokens for {@code /v1/**} and {@code /admin/v1/**} (spec §4.2): signature with Auth's own keys,
 * audience, issuer, expiry, and neither the client nor the jti revoked (10 s caches).
 */
public class ApiAuthFilter extends OncePerRequestFilter {

  private final TokenSigner signer;
  private final KeyRing keys;
  private final Clock clock;
  private final Supplier<Set<String>> disabledClients;
  private final Supplier<Set<String>> revokedJtis;

  public ApiAuthFilter(TokenSigner signer, KeyRing keys, Clock clock, Supplier<Set<String>> disabledClients,
      Supplier<Set<String>> revokedJtis) {
    this.signer = signer;
    this.keys = keys;
    this.clock = clock;
    this.disabledClients = disabledClients;
    this.revokedJtis = revokedJtis;
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    String h = req.getHeader(HttpHeaders.AUTHORIZATION);
    Optional<ApiClaims> claims = h != null && h.regionMatches(true, 0, "Bearer ", 0, 7)
        ? signer.verifyApi(h.substring(7).trim(), keys::publicKey, clock.now()) : Optional.empty();
    if (claims.isEmpty() || disabledClients.get().contains(claims.get().clientId())
        || revokedJtis.get().contains(claims.get().jti())) {
      res.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer error=\"invalid_token\"");
      Problems.write(res, 401, "invalid_token", "missing, invalid, expired or revoked access token");
      return;
    }
    ApiClaims c = claims.get();
    req.setAttribute(ApiPrincipal.ATTR, new Caller(c.clientId(), c.tenant(), c.scopes(), c.jti()));
    chain.doFilter(req, res);
  }
}
