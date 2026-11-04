package com.pangate.gateway.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.interfaces.ECPublicKey;
import java.util.Date;
import java.util.UUID;

/**
 * Connect tokens for the warm-up client (spec §7.6), signed with an ES256 key that exists only in this process.
 * They carry the reserved tenant {@value #TENANT}; the gateway accepts them only from loopback and keeps their
 * sessions out of the registry, Kafka and business metrics.
 */
public final class WarmUpTokens {

  public static final String TENANT = "__warmup";

  private final ECKey key;
  private final String audience;
  private final String issuer;

  public WarmUpTokens(String audience, String issuer) {
    this.audience = audience;
    this.issuer = issuer == null ? "" : issuer;
    try {
      this.key = new ECKeyGenerator(Curve.P_256).keyID("warmup-" + UUID.randomUUID()).generate();
    } catch (JOSEException e) {
      throw new IllegalStateException("cannot generate the warm-up key", e);
    }
  }

  public String kid() {
    return key.getKeyID();
  }

  public ECPublicKey publicKey() {
    try {
      return key.toECPublicKey();
    } catch (JOSEException e) {
      throw new IllegalStateException(e);
    }
  }

  public String mint(int i, long nowSec) {
    JWTClaimsSet.Builder c = new JWTClaimsSet.Builder()
        .subject(TENANT + ":" + i)
        .claim("tenant", TENANT)
        .claim("device", "warmup-" + i)
        .jwtID(UUID.randomUUID().toString())
        .audience(audience)
        .notBeforeTime(new Date((nowSec - 1) * 1000))
        .expirationTime(new Date((nowSec + 60) * 1000));
    if (!issuer.isEmpty()) {
      c.issuer(issuer);
    }
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(kid()).build(), c.build());
    try {
      jwt.sign(new ECDSASigner(key));
    } catch (JOSEException e) {
      throw new IllegalStateException(e);
    }
    return jwt.serialize();
  }

  public static boolean isWarmUp(String tenantId) {
    return TENANT.equals(tenantId);
  }
}
