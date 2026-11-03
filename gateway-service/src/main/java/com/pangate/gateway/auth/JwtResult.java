package com.pangate.gateway.auth;

/** Outcome of verifying a connect JWT. */
public sealed interface JwtResult {

  record Verified(ConnectClaims claims) implements JwtResult {}

  /** Signed with a key the cache does not know yet: refresh JWKS, then verify again. */
  record UnknownKid(String kid) implements JwtResult {}

  record Rejected(String reason) implements JwtResult {}
}
