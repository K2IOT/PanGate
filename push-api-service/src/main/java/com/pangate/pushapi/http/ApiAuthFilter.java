package com.pangate.pushapi.http;

import com.pangate.pushapi.auth.ApiTokenVerifier;
import com.pangate.pushapi.auth.InvalidTokenException;
import com.pangate.pushapi.auth.RevokedTokenException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/** Bearer API token on {@code /v1/**} and {@code /admin/v1/**} → request attribute {@link #CALLER}. */
public class ApiAuthFilter extends OncePerRequestFilter {

  public static final String CALLER = "pushapi.caller";

  private final ApiTokenVerifier verifier;

  public ApiAuthFilter(ApiTokenVerifier verifier) {
    this.verifier = verifier;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest req) {
    String p = req.getRequestURI();
    return !(p.startsWith("/v1/") || p.startsWith("/admin/v1/"));
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
    try {
      req.setAttribute(CALLER, verifier.verify(h.substring(7).trim()));
    } catch (InvalidTokenException e) {
      res.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
      Problems.write(res, 401, "invalid_token", e.getMessage());
      return;
    } catch (RevokedTokenException e) {
      Problems.write(res, 403, "token_revoked", e.getMessage());
      return;
    }
    chain.doFilter(req, res);
  }
}
