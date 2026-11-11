package com.pangate.auth.application;

import com.pangate.auth.application.port.ClientStore;
import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.SecretHasher;
import com.pangate.auth.application.port.TenantStore;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.domain.ApiClaims;
import com.pangate.auth.domain.Client;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.domain.Tenant;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** OAuth2 client credentials (spec §4.1). */
public final class IssueClientToken {

  public record IssuedToken(String accessToken, long expiresIn, String scope) {}

  private final ClientStore clients;
  private final TenantStore tenants;
  private final SecretHasher hasher;
  private final TokenSigner signer;
  private final KeyRing keys;
  private final Clock clock;
  private final String issuer;
  private final Duration ttl;

  public IssueClientToken(ClientStore clients, TenantStore tenants, SecretHasher hasher, TokenSigner signer,
      KeyRing keys, Clock clock, String issuer, Duration ttl) {
    this.clients = clients;
    this.tenants = tenants;
    this.hasher = hasher;
    this.signer = signer;
    this.keys = keys;
    this.clock = clock;
    this.issuer = issuer;
    this.ttl = ttl;
  }

  public IssuedToken issue(String clientId, char[] secret, String requestedScope) {
    Optional<Client> found = clients.find(clientId);
    if (found.isEmpty()) {
      hasher.burn();  // same latency as a wrong secret
      throw OAuthError.invalidClient();
    }
    Client c = found.get();
    if (!hasher.matches(secret, c.secretHash()) && !hasher.matches(secret, c.secretHashNext())) {
      throw OAuthError.invalidClient();
    }
    if (!c.active() || !tenants.find(c.tenantId()).map(Tenant::active).orElse(false)) {
      throw OAuthError.invalidClient();
    }
    Set<Scope> assigned = EnumSet.noneOf(Scope.class);
    assigned.addAll(c.scopes());
    if (!Tenant.isOperator(c.tenantId())) {
      assigned.remove(Scope.ADMIN);  // never, whatever the row says (spec §4.1)
    }
    Set<Scope> granted;
    try {
      Set<Scope> requested = Scope.parse(requestedScope);
      granted = requested.isEmpty() ? assigned : requested;
    } catch (DomainException e) {
      throw new OAuthError("invalid_scope", 400, "unknown scope");
    }
    if (!assigned.containsAll(granted)) {
      throw new OAuthError("invalid_scope", 400, "scope exceeds the client's assignment");
    }
    Instant now = clock.now().truncatedTo(ChronoUnit.SECONDS);
    ApiClaims claims = new ApiClaims(issuer, c.id(), c.tenantId(), granted, UUID.randomUUID().toString(), now,
        now.plus(ttl));
    return new IssuedToken(signer.sign(keys.active(), claims), ttl.toSeconds(), Scope.format(granted));
  }
}
