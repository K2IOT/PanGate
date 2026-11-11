package com.pangate.auth.infrastructure.crypto;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.domain.ApiClaims;
import com.pangate.auth.domain.ConnectClaims;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.domain.SigningKey;
import java.security.interfaces.ECPublicKey;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/** Nimbus ES256 signer (spec §4.1, §4.3). */
public final class Es256TokenSigner implements TokenSigner {

  static final Duration SKEW = Duration.ofSeconds(30);

  private final String issuer;

  public Es256TokenSigner(String issuer) {
    this.issuer = issuer;
  }

  /** A fresh P-256 key whose kid is its RFC 7638 thumbprint. */
  public static SigningKey newKey(Instant activateAt, KeyState state) {
    try {
      ECKey k = new ECKeyGenerator(Curve.P_256).keyUse(KeyUse.SIGNATURE).algorithm(JWSAlgorithm.ES256).generate();
      String kid = k.computeThumbprint().toString();
      ECKey withKid = new ECKey.Builder(k).keyID(kid).build();
      return new SigningKey(kid, withKid, state, activateAt, null, null);
    } catch (JOSEException e) {
      throw new IllegalStateException("cannot generate an ES256 key", e);
    }
  }

  @Override
  public String sign(SigningKey key, ApiClaims c) {
    return sign(key, new JWTClaimsSet.Builder()
        .issuer(c.issuer())
        .audience(ApiClaims.AUDIENCE)
        .subject("client:" + c.clientId())
        .claim("tenant", c.tenant())
        .claim("client_id", c.clientId())
        .claim("scope", Scope.format(c.scopes()))
        .jwtID(c.jti())
        .issueTime(Date.from(c.issuedAt()))
        .notBeforeTime(Date.from(c.issuedAt()))
        .expirationTime(Date.from(c.expiresAt()))
        .build());
  }

  @Override
  public String sign(SigningKey key, ConnectClaims c) {
    var r = c.req();
    return sign(key, new JWTClaimsSet.Builder()
        .issuer(c.issuer())
        .audience(ConnectClaims.AUDIENCE)
        .subject(r.principal())
        .claim("tenant", r.tenant())
        .claim("device", r.device())
        .claim("topics", r.topics())
        .jwtID(c.jti())
        .issueTime(Date.from(c.issuedAt()))
        .notBeforeTime(Date.from(c.issuedAt()))
        .expirationTime(Date.from(c.expiresAt()))
        .build());
  }

  private static String sign(SigningKey key, JWTClaimsSet claims) {
    try {
      SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.kid())
          .type(JOSEObjectType.JWT).build(), claims);
      jwt.sign(new ECDSASigner(key.privateJwk()));
      return jwt.serialize();
    } catch (JOSEException e) {
      throw new IllegalStateException("cannot sign with " + key.kid(), e);
    }
  }

  @Override
  public Optional<ApiClaims> verifyApi(String token, Function<String, Optional<ECPublicKey>> keyByKid, Instant now) {
    try {
      SignedJWT jwt = SignedJWT.parse(token);
      if (!JWSAlgorithm.ES256.equals(jwt.getHeader().getAlgorithm()) || jwt.getHeader().getKeyID() == null) {
        return Optional.empty();
      }
      Optional<ECPublicKey> key = keyByKid.apply(jwt.getHeader().getKeyID());
      if (key.isEmpty() || !jwt.verify(new ECDSAVerifier(key.get()))) {
        return Optional.empty();
      }
      JWTClaimsSet c = jwt.getJWTClaimsSet();
      if (!List.of(ApiClaims.AUDIENCE).equals(c.getAudience()) || !issuer.equals(c.getIssuer())
          || c.getExpirationTime() == null || c.getNotBeforeTime() == null || c.getIssueTime() == null) {
        return Optional.empty();
      }
      Instant exp = c.getExpirationTime().toInstant();
      Instant nbf = c.getNotBeforeTime().toInstant();
      if (!now.isBefore(exp.plus(SKEW)) || now.plus(SKEW).isBefore(nbf)) {
        return Optional.empty();
      }
      String clientId = c.getStringClaim("client_id");
      String tenant = c.getStringClaim("tenant");
      if (clientId == null || tenant == null || c.getJWTID() == null
          || !("client:" + clientId).equals(c.getSubject())) {
        return Optional.empty();
      }
      return Optional.of(new ApiClaims(c.getIssuer(), clientId, tenant, Scope.parse(c.getStringClaim("scope")),
          c.getJWTID(), c.getIssueTime().toInstant(), exp));
    } catch (ParseException | JOSEException | RuntimeException e) {
      return Optional.empty();
    }
  }
}
