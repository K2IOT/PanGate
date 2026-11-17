package com.pangate.account.api;

import com.pangate.account.application.Caller;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.infrastructure.auth.ConnectTokenVerifier;
import com.pangate.account.infrastructure.auth.InvalidTokenException;
import com.pangate.account.infrastructure.auth.RevokedTokenException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Connect token on {@code /v1/**} (account spec §2), except register, log in and exchange. After the token checks
 * (signature, audience, tenant chat, revocations from gw.control) the session of its device must still be live in
 * the database; a live answer is cached 10 s, so a device logged out elsewhere is refused within 10 s.
 */
public class ConnectAuthFilter extends OncePerRequestFilter {

  public static final String CALLER = "account.caller";
  static final long LIVE_CACHE_MS = 10_000;

  private final ConnectTokenVerifier verifier;
  private final SessionStore sessions;
  private final LongSupplier monoMs;
  private final Map<String, Long> liveUntil = new ConcurrentHashMap<>();

  public ConnectAuthFilter(ConnectTokenVerifier verifier, SessionStore sessions, LongSupplier monoMs) {
    this.verifier = verifier;
    this.sessions = sessions;
    this.monoMs = monoMs;
  }

  static boolean open(String method, String path) {
    return "POST".equals(method)
        && (path.equals("/v1/accounts") || path.equals("/v1/sessions") || path.equals("/v1/sessions/token"));
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest req) {
    String path = req.getRequestURI().substring(req.getContextPath().length());
    return !path.startsWith("/v1/") || open(req.getMethod(), path);
  }

  public static Caller caller(HttpServletRequest req) {
    return (Caller) req.getAttribute(CALLER);
  }

  /** Forgets the cached answer for a device logged out through this pod. */
  public void forget(String userId, String deviceId) {
    liveUntil.remove(userId + "|" + deviceId);
  }

  private boolean live(String userId, String deviceId) {
    long now = monoMs.getAsLong();
    String key = userId + "|" + deviceId;
    Long until = liveUntil.get(key);
    if (until != null && until > now) {
      return true;
    }
    if (!sessions.isLive(userId, deviceId)) {
      liveUntil.remove(key);
      return false;
    }
    if (liveUntil.size() > 100_000) {
      liveUntil.clear();
    }
    liveUntil.put(key, now + LIVE_CACHE_MS);
    return true;
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
    ConnectTokenVerifier.Caller c;
    try {
      c = verifier.verify(h.substring(7).trim());
    } catch (InvalidTokenException | RevokedTokenException e) {
      res.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
      Problems.write(res, 401, "invalid_token", e.getMessage());
      return;
    }
    if (!live(c.userId(), c.deviceId())) {
      res.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
      Problems.write(res, 401, "invalid_token", "this device was logged out");
      return;
    }
    req.setAttribute(CALLER, new Caller(c.userId(), c.deviceId(), ClientIp.of(req)));
    chain.doFilter(req, res);
  }
}
