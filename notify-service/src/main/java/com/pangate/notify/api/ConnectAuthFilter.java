package com.pangate.notify.api;

import com.pangate.notify.application.Caller;
import com.pangate.notify.infrastructure.auth.ConnectTokenVerifier;
import com.pangate.notify.infrastructure.auth.InvalidTokenException;
import com.pangate.notify.infrastructure.auth.RevokedTokenException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Connect token on {@code /v1/**} (offline notifications spec §5.1): signature, audience, expiry and the revocations
 * of {@code gw.control}. A revocation that arrives later removes the token (§5.3), so no session lookup is needed.
 */
public class ConnectAuthFilter extends OncePerRequestFilter {

  public static final String CALLER = "notify.caller";

  private final ConnectTokenVerifier verifier;

  public ConnectAuthFilter(ConnectTokenVerifier verifier) {
    this.verifier = verifier;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest req) {
    return !req.getRequestURI().substring(req.getContextPath().length()).startsWith("/v1/");
  }

  public static Caller caller(HttpServletRequest req) {
    return (Caller) req.getAttribute(CALLER);
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    String h = req.getHeader("Authorization");
    if (h == null || !h.regionMatches(true, 0, "Bearer ", 0, 7) || h.length() <= 7) {
      res.setHeader("WWW-Authenticate", "Bearer");
      Problems.write(res, 401, "invalid_token", "a Bearer connect token is required");
      return;
    }
    try {
      req.setAttribute(CALLER, verifier.verify(h.substring(7).trim()));
    } catch (InvalidTokenException | RevokedTokenException e) {
      res.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
      Problems.write(res, 401, "invalid_token", e.getMessage());
      return;
    }
    chain.doFilter(req, res);
  }
}
