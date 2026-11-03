package com.pangate.gateway.auth;

import com.pangate.gateway.control.CellMapView;
import com.pangate.gateway.control.RevocationList;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.control.TenantPolicy;
import com.pangate.gateway.protocol.CloseCode;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Admission checks after the credential verified (spec §4.6 step 3, c–h, in order; the first failing one wins):
 * cell, revocation, tenant blocked, minimum SDK, Origin allowlist, per-principal handshake rate. Check i (tenant
 * connection cap) arrives with M3.
 */
public final class HelloPolicy {

  static final long BLOCKED_RETRY_AFTER_MS = 300_000;

  /** {@code ticketCell} is the resume ticket's cell, or this node's cell for a JWT; {@code origin} may be null. */
  public record Candidate(String ticketCell, String tenant, String principal, String device, long jtiHash,
      String sdkVersion, String origin) {}

  public record Rejection(CloseCode code, long retryAfterMs, String metricResult) {}

  private final String cell;
  private final CellMapView cells;
  private final RevocationList revocations;
  private final TenantLimitsStore tenants;
  private final PrincipalHandshakeLimiter limiter;
  private final LongSupplier wallMillis;

  public HelloPolicy(String cell, CellMapView cells, RevocationList revocations, TenantLimitsStore tenants,
      PrincipalHandshakeLimiter limiter, LongSupplier wallMillis) {
    this.cell = cell;
    this.cells = cells;
    this.revocations = revocations;
    this.tenants = tenants;
    this.limiter = limiter;
    this.wallMillis = wallMillis;
  }

  /** HELLO: checks c–h; the rate budget (h) is only consumed when c–g passed. */
  public Optional<Rejection> check(Candidate c) {
    Optional<Rejection> r = checkPolicy(c);
    if (r.isPresent()) {
      return r;
    }
    if (!limiter.tryAcquire(c.principal())) {
      return Optional.of(new Rejection(CloseCode.RATE_LIMITED, CloseCode.RATE_LIMITED.defaultRetryAfterMs(),
          "rate_limited"));
    }
    return Optional.empty();
  }

  /** Checks c–g only (REAUTH applies the rate limit itself, before the ES256 verify it protects). */
  public Optional<Rejection> checkPolicy(Candidate c) {
    if (!cell.equals(c.ticketCell()) || !cells.servesTenant(c.tenant())) {
      return Optional.of(new Rejection(CloseCode.WRONG_CELL, CloseCode.WRONG_CELL.defaultRetryAfterMs(),
          "wrong_cell"));
    }
    if (revocations.isRevoked(c.principal(), c.device(), c.jtiHash(), wallMillis.getAsLong())) {
      return Optional.of(new Rejection(CloseCode.REVOKED, 0, "revoked"));
    }
    TenantPolicy p = tenants.policy(c.tenant());
    if (p.blocked()) {
      return Optional.of(new Rejection(CloseCode.RATE_LIMITED, BLOCKED_RETRY_AFTER_MS, "blocked"));
    }
    if (!p.minSdkVersion().isEmpty() && SdkVersion.compare(c.sdkVersion(), p.minSdkVersion()) < 0) {
      return Optional.of(new Rejection(CloseCode.UPGRADE_REQUIRED, 0, "sdk_too_old"));
    }
    if (c.origin() != null && !p.originAllowlist().isEmpty() && !p.originAllowlist().contains(c.origin())) {
      return Optional.of(new Rejection(CloseCode.AUTH_FAILED, 0, "origin"));
    }
    return Optional.empty();
  }
}
