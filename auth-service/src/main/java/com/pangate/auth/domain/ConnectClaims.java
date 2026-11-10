package com.pangate.auth.domain;

import java.time.Instant;

/** Claims of a gateway connect token ({@code aud=pangate-gw}, spec §4.3). */
public record ConnectClaims(String issuer, ConnectTokenRequest req, String jti, Instant issuedAt, Instant expiresAt) {

  public static final String AUDIENCE = "pangate-gw";
}
