package com.pangate.chat.http;

import com.pangate.chat.auth.ConnectTokenVerifier.Caller;
import com.pangate.chat.auth.InvalidTokenException;
import com.pangate.chat.auth.RevokedTokenException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/** Bearer connect token on {@code /v1/**} (chat spec §7) → request attribute {@link #CALLER}. */
public class AuthFilter extends OncePerRequestFilter {

  public static final String CALLER = "chat.caller";

  /** {@code ConnectTokenVerifier::verify}. */
  @FunctionalInterface
  public interface Verifier {
    Caller verify(String token);
  }

  private final Verifier verifier;

  public AuthFilter(Verifier verifier) {
    this.verifier = verifier;
  }

  public static Caller caller(HttpServletRequest req) {
    return (Caller) req.getAttribute(CALLER);
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest req) {
    return !req.getRequestURI().startsWith("/v1/");
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
    } catch (InvalidTokenException e) {
      res.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
      Problems.write(res, 401, "invalid_token", e.getMessage());
      return;
    } catch (RevokedTokenException e) {
      Problems.write(res, 403, "forbidden", e.getMessage());
      return;
    }
    chain.doFilter(req, res);
  }
}
