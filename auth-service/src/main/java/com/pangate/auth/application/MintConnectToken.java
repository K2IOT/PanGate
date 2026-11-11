package com.pangate.auth.application;

import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.TenantStore;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.domain.Caller;
import com.pangate.auth.domain.ClaimLimits;
import com.pangate.auth.domain.ConnectClaims;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.domain.Tenant;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/** {@code POST /v1/tokens}: a connect token for the gateway, inside the caller's tenant (spec §4.3). */
public final class MintConnectToken {

  public record MintedToken(String token, Instant expiresAt) {}

  private final TenantStore tenants;
  private final TokenSigner signer;
  private final KeyRing keys;
  private final Clock clock;
  private final String issuer;
  private final int maxTtlSec;

  public MintConnectToken(TenantStore tenants, TokenSigner signer, KeyRing keys, Clock clock, String issuer,
      Duration maxTtl) {
    this.tenants = tenants;
    this.signer = signer;
    this.keys = keys;
    this.clock = clock;
    this.issuer = issuer;
    this.maxTtlSec = (int) Math.min(maxTtl.toSeconds(), ClaimLimits.TTL_MAX);
  }

  public MintedToken mint(Caller caller, String principal, String device, List<String> topics, Integer ttlSec) {
    caller.require(Scope.TOKENS_MINT);
    var req = ClaimLimits.validate(caller.tenant(), principal, device, topics,
        ttlSec == null ? maxTtlSec : ttlSec);
    if (req.ttlSec() > maxTtlSec) {
      throw DomainException.badRequest("ttl_out_of_range", "ttlSec must be at most " + maxTtlSec);
    }
    if (!tenants.find(caller.tenant()).map(Tenant::active).orElse(false)) {
      throw new DomainException("tenant_disabled", 403, "tenant " + caller.tenant() + " is disabled");
    }
    Instant now = clock.now().truncatedTo(ChronoUnit.SECONDS);
    Instant exp = now.plusSeconds(req.ttlSec());
    String token = signer.sign(keys.active(), new ConnectClaims(issuer, req, UUID.randomUUID().toString(), now, exp));
    return new MintedToken(token, exp);
  }
}
