package com.pangate.gateway.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.nio.charset.StandardCharsets;
import java.security.interfaces.ECPublicKey;
import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/** Offline ES256 connect-token verification against {@link JwksCache} (spec §4.6). Cheap enough for the loop. */
public final class JwtVerifier {

  /** UTF-8 byte caps that keep a resume ticket within its fixed buffer (spec §4.7, 2 KB). */
  static final int MAX_TENANT_BYTES = 64;
  static final int MAX_PRINCIPAL_BYTES = 256;
  static final int MAX_DEVICE_BYTES = 128;

  private final JwksCache cache;
  private final String audience;
  private final String issuer;
  private final long skewSec;

  public JwtVerifier(JwksCache cache, String audience, String issuerOrEmpty, Duration clockSkew) {
    this.cache = cache;
    this.audience = audience;
    this.issuer = issuerOrEmpty == null ? "" : issuerOrEmpty;
    this.skewSec = clockSkew.toSeconds();
  }

  public JwtResult verify(String jwt, long nowSec) {
    SignedJWT parsed;
    try {
      parsed = SignedJWT.parse(jwt);
    } catch (ParseException | RuntimeException e) {
      return new JwtResult.Rejected("malformed token");
    }
    if (!JWSAlgorithm.ES256.equals(parsed.getHeader().getAlgorithm())) {
      return new JwtResult.Rejected("alg must be ES256");
    }
    String kid = parsed.getHeader().getKeyID();
    if (kid == null || kid.isBlank()) {
      return new JwtResult.Rejected("missing kid");
    }
    Optional<ECPublicKey> key = cache.key(kid);
    if (key.isEmpty()) {
      return new JwtResult.UnknownKid(kid);
    }
    try {
      if (!parsed.verify(new ECDSAVerifier(key.get()))) {
        return new JwtResult.Rejected("bad signature");
      }
      return checkClaims(parsed.getJWTClaimsSet(), nowSec);
    } catch (JOSEException | ParseException | RuntimeException e) {
      return new JwtResult.Rejected("invalid token: " + e.getMessage());
    }
  }

  private JwtResult checkClaims(JWTClaimsSet c, long nowSec) throws ParseException {
    if (c.getAudience() == null || !c.getAudience().contains(audience)) {
      return new JwtResult.Rejected("wrong audience");
    }
    if (!issuer.isEmpty() && !issuer.equals(c.getIssuer())) {
      return new JwtResult.Rejected("wrong issuer");
    }
    Date exp = c.getExpirationTime();
    Date nbf = c.getNotBeforeTime();
    if (exp == null || nbf == null) {
      return new JwtResult.Rejected("exp and nbf are required");
    }
    long expSec = exp.getTime() / 1000;
    if (nowSec > expSec + skewSec) {
      return new JwtResult.Rejected("token expired");
    }
    if (nbf.getTime() / 1000 > nowSec + skewSec) {
      return new JwtResult.Rejected("token not yet valid");
    }
    String sub = c.getSubject();
    String tenant = c.getStringClaim("tenant");
    String device = c.getStringClaim("device");
    String jti = c.getJWTID();
    if (blank(sub) || blank(tenant) || blank(device) || blank(jti)) {
      return new JwtResult.Rejected("sub, tenant, device and jti are required");
    }
    if (bytes(tenant) > MAX_TENANT_BYTES || bytes(sub) > MAX_PRINCIPAL_BYTES || bytes(device) > MAX_DEVICE_BYTES) {
      return new JwtResult.Rejected("tenant, sub or device too long");
    }
    if (!sub.startsWith(tenant + ":")) {
      return new JwtResult.Rejected("subject is outside the token's tenant");
    }
    List<String> topics = c.getStringListClaim("topics");
    topics = topics == null ? List.of() : topics;
    if (topics.size() > ResumeTicket.MAX_PATTERNS) {
      return new JwtResult.Rejected("too many topic patterns");
    }
    for (String t : topics) {
      if (t == null || t.getBytes(StandardCharsets.UTF_8).length > ResumeTicket.MAX_PATTERN_BYTES) {
        return new JwtResult.Rejected("topic pattern too long");
      }
    }
    return new JwtResult.Verified(new ConnectClaims(tenant, sub, device, jti, topics, expSec));
  }

  private static int bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8).length;
  }

  private static boolean blank(String s) {
    return s == null || s.isBlank();
  }
}
