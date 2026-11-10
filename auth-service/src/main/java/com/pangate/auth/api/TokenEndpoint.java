package com.pangate.auth.api;

import com.pangate.auth.application.IssueClientToken;
import com.pangate.auth.application.NoSigningKeyException;
import com.pangate.auth.application.OAuthError;
import com.pangate.auth.infrastructure.AuthMetrics;
import com.pangate.auth.infrastructure.ratelimit.FailedAuthTracker;
import com.pangate.auth.infrastructure.ratelimit.TokenBuckets;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /oauth2/token}, client credentials only (RFC 6749 §4.4, spec §4.1). */
@RestController
public class TokenEndpoint {

  private final IssueClientToken issue;
  private final TokenBuckets buckets;
  private final FailedAuthTracker failures;
  private final AuthMetrics metrics;

  public TokenEndpoint(IssueClientToken issue, TokenBuckets buckets, FailedAuthTracker failures, AuthMetrics metrics) {
    this.issue = issue;
    this.buckets = buckets;
    this.failures = failures;
    this.metrics = metrics;
  }

  /** Too many requests; carries Retry-After. */
  static final class Limited extends RuntimeException {
    final long retryAfterSec;

    Limited(long ms) {
      super(null, null, false, false);
      this.retryAfterSec = RateLimitFilter.retryAfterSeconds(ms);
    }
  }

  private record Credentials(String id, String secret) {}

  @PostMapping(value = "/oauth2/token", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  ResponseEntity<Map<String, Object>> token(HttpServletRequest req) {
    long start = System.nanoTime();
    String source = req.getRemoteAddr();
    failures.blockedFor(source).ifPresent(ms -> {
      metrics.rateLimited("token");
      throw new Limited(ms);
    });
    Credentials cred;
    try {
      cred = credentials(req);
    } catch (OAuthError e) {
      if ("invalid_client".equals(e.error())) {
        failures.recordFailure(source);
      }
      throw e;
    }
    buckets.tryAcquire("token", cred.id()).ifPresent(ms -> {
      metrics.rateLimited("token");
      throw new Limited(ms);
    });
    String grant = req.getParameter("grant_type");
    if (grant == null) {
      throw new OAuthError("invalid_request", 400, "grant_type is required");
    }
    if (!"client_credentials".equals(grant)) {
      throw new OAuthError("unsupported_grant_type", 400, "only client_credentials is supported");
    }
    IssueClientToken.IssuedToken t;
    try {
      t = issue.issue(cred.id(), cred.secret().toCharArray(), req.getParameter("scope"));
    } catch (OAuthError e) {
      metrics.issued("api", e.error());
      if ("invalid_client".equals(e.error())) {
        failures.recordFailure(source);
      }
      throw e;
    }
    metrics.issued("api", "ok");
    metrics.latency("api", start);
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("access_token", t.accessToken());
    body.put("token_type", "Bearer");
    body.put("expires_in", t.expiresIn());
    body.put("scope", t.scope());
    return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(body);
  }

  /** Exactly one of HTTP Basic (form-urlencoded parts, RFC 6749 §2.3.1) or form parameters. */
  static Credentials credentials(HttpServletRequest req) {
    String h = req.getHeader(HttpHeaders.AUTHORIZATION);
    boolean basic = h != null && h.regionMatches(true, 0, "Basic ", 0, 6);
    String formId = req.getParameter("client_id");
    String formSecret = req.getParameter("client_secret");
    boolean form = formId != null || formSecret != null;
    if (basic == form) {
      throw new OAuthError("invalid_request", 400, "use exactly one of HTTP Basic or client_id/client_secret");
    }
    if (form) {
      if (formId == null || formSecret == null) {
        throw new OAuthError("invalid_request", 400, "client_id and client_secret are both required");
      }
      return new Credentials(formId, formSecret);
    }
    try {
      String decoded = new String(Base64.getDecoder().decode(h.substring(6).trim()), StandardCharsets.UTF_8);
      int colon = decoded.indexOf(':');
      if (colon < 0) {
        throw OAuthError.invalidClient();
      }
      return new Credentials(URLDecoder.decode(decoded.substring(0, colon), StandardCharsets.UTF_8),
          URLDecoder.decode(decoded.substring(colon + 1), StandardCharsets.UTF_8));
    } catch (IllegalArgumentException e) {
      throw OAuthError.invalidClient();
    }
  }

  @ExceptionHandler(Limited.class)
  ResponseEntity<Map<String, String>> limited(Limited e) {
    return ResponseEntity.status(429).cacheControl(CacheControl.noStore()).contentType(MediaType.APPLICATION_JSON)
        .header(HttpHeaders.RETRY_AFTER, Long.toString(e.retryAfterSec))
        .body(Map.of("error", "rate_limited", "error_description", "too many requests"));
  }

  /** PostgreSQL down or no signing key (spec §6). */
  @ExceptionHandler({DataAccessException.class, TransactionException.class, NoSigningKeyException.class})
  ResponseEntity<Map<String, String>> unavailable(Exception e) {
    metrics.issued("api", "unavailable");
    return ResponseEntity.status(503).cacheControl(CacheControl.noStore()).contentType(MediaType.APPLICATION_JSON)
        .header(HttpHeaders.RETRY_AFTER, "5")
        .body(Map.of("error", "temporarily_unavailable", "error_description", "try again shortly"));
  }

  @ExceptionHandler(OAuthError.class)
  ResponseEntity<Map<String, String>> error(OAuthError e) {
    var b = ResponseEntity.status(e.status()).cacheControl(CacheControl.noStore())
        .contentType(MediaType.APPLICATION_JSON);
    if (e.status() == 401) {
      b.header(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"pangate\"");
    }
    return b.body(Map.of("error", e.error(), "error_description", e.getMessage()));
  }
}
