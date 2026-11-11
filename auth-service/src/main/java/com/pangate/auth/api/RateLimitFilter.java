package com.pangate.auth.api;

import com.pangate.auth.domain.Caller;
import com.pangate.auth.infrastructure.AuthMetrics;
import com.pangate.auth.infrastructure.ratelimit.TokenBuckets;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.OptionalLong;
import org.springframework.http.HttpHeaders;
import org.springframework.web.filter.OncePerRequestFilter;

/** Per-client limits of {@code /v1/tokens}, {@code /v1/revocations} and {@code /admin/v1/**} (spec §7). */
public class RateLimitFilter extends OncePerRequestFilter {

  private final TokenBuckets buckets;
  private final AuthMetrics metrics;

  public RateLimitFilter(TokenBuckets buckets, AuthMetrics metrics) {
    this.buckets = buckets;
    this.metrics = metrics;
  }

  static String bucket(String path) {
    if (path.startsWith("/admin/v1/")) {
      return "admin";
    }
    if (path.equals("/v1/tokens")) {
      return "mint";
    }
    if (path.equals("/v1/revocations")) {
      return "revoke";
    }
    return null;
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    String bucket = bucket(req.getRequestURI());
    if (bucket != null && req.getAttribute(ApiPrincipal.ATTR) instanceof Caller c) {
      OptionalLong wait = buckets.tryAcquire(bucket, c.clientId());
      if (wait.isPresent()) {
        metrics.rateLimited(bucket);
        res.setHeader(HttpHeaders.RETRY_AFTER, Long.toString(retryAfterSeconds(wait.getAsLong())));
        Problems.write(res, 429, "rate_limited", "too many requests");
        return;
      }
    }
    chain.doFilter(req, res);
  }

  static long retryAfterSeconds(long ms) {
    return Math.max(1, (ms + 999) / 1000);
  }
}
