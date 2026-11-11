package com.pangate.auth.application.port;

import com.pangate.auth.domain.ApiClaims;
import com.pangate.auth.domain.ConnectClaims;
import com.pangate.auth.domain.SigningKey;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

/** ES256 signing of API and connect tokens, and verification of API tokens. */
public interface TokenSigner {

  String sign(SigningKey key, ApiClaims claims);

  String sign(SigningKey key, ConnectClaims claims);

  /** Signature (key by kid), {@code aud=pangate-api}, issuer, exp/nbf with 30 s skew; any failure is empty. */
  Optional<ApiClaims> verifyApi(String token, Function<String, Optional<ECPublicKey>> keyByKid, Instant now);
}
