package com.pangate.devtools.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

/** Mints ES256 connect JWTs with the claims the gateway requires (spec §4.6). */
public final class JwtMinter {

  public static final String AUDIENCE = "pangate-gw";

  private final ECKey key;
  private final String issuer;

  public JwtMinter(ECKey key, String issuer) {
    this.key = key;
    this.issuer = issuer;
  }

  public String mint(TokenRequest req, Instant now) {
    JWTClaimsSet claims = new JWTClaimsSet.Builder()
        .issuer(issuer)
        .audience(AUDIENCE)
        .subject(req.principal())
        .claim("tenant", req.tenant())
        .claim("device", req.device())
        .claim("topics", req.topics())
        .jwtID(UUID.randomUUID().toString())
        .issueTime(Date.from(now))
        .notBeforeTime(Date.from(now))
        .expirationTime(Date.from(now.plus(req.ttl())))
        .build();
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.getKeyID()).build(), claims);
    try {
      jwt.sign(new ECDSASigner(key));
    } catch (JOSEException e) {
      throw new IllegalStateException("Cannot sign token", e);
    }
    return jwt.serialize();
  }
}
