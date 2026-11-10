package com.pangate.auth.domain;

import java.time.Instant;
import java.util.Set;

/** Claims of an API access token ({@code aud=pangate-api}, spec §4.1). */
public record ApiClaims(String issuer, String clientId, String tenant, Set<Scope> scopes, String jti,
    Instant issuedAt, Instant expiresAt) {

  public static final String AUDIENCE = "pangate-api";

  public ApiClaims {
    scopes = Set.copyOf(scopes);
  }
}
