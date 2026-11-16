package com.pangate.account.api;

import com.pangate.account.application.port.AttemptCounter;
import com.pangate.account.infrastructure.auth.ApiTokenVerifier;
import com.pangate.account.infrastructure.auth.InvalidTokenException;
import com.pangate.account.infrastructure.auth.RevokedTokenException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * {@code /admin/v1/**} (account spec §5): an auth-service API token with scope {@code accounts:admin} of the chat
 * tenant; 20 requests a second per client (spec §7). Sets the client id as the actor of admin_actions.
 */
public class AdminAuthFilter extends OncePerRequestFilter {

  public static final String ACTOR = "account.admin.actor";

  private final ApiTokenVerifier verifier;
  private final String tenant;
  private final AttemptCounter counter;
  private final int perSecond;

  public AdminAuthFilter(ApiTokenVerifier verifier, String tenant, AttemptCounter counter, int perSecond) {
    this.verifier = verifier;
    this.tenant = tenant;
    this.counter = counter;
    this.perSecond = perSecond;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest req) {
    return !req.getRequestURI().substring(req.getContextPath().length()).startsWith("/admin/v1/");
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    String h = req.getHeader("Authorization");
    if (h == null || !h.regionMatches(true, 0, "Bearer ", 0, 7) || h.length() <= 7) {
      res.setHeader("WWW-Authenticate", "Bearer");
      Problems.write(res, 401, "invalid_token", "a Bearer API token is required");
      return;
    }
    ApiTokenVerifier.ApiCaller c;
    try {
      c = verifier.verify(h.substring(7).trim());
    } catch (InvalidTokenException | RevokedTokenException e) {
      res.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
      Problems.write(res, 401, "invalid_token", e.getMessage());
      return;
    }
    if (!tenant.equals(c.tenant()) || !c.scopes().contains("accounts:admin")) {
      Problems.write(res, 403, "forbidden", "scope accounts:admin of tenant " + tenant + " is required");
      return;
    }
    if (counter.increment("adm:" + c.clientId(), Duration.ofSeconds(1)) > perSecond) {
      res.setHeader("Retry-After", "1");
      Problems.write(res, 429, "rate_limited", "too many admin requests");
      return;
    }
    req.setAttribute(ACTOR, c.clientId());
    chain.doFilter(req, res);
  }
}
