package com.pangate.pushapi.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.pushapi.core.Caller;
import com.pangate.pushapi.core.ControlState;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API tokens of auth-service (auth spec §3: ES256, {@code aud=pangate-api}, claims {@code tenant}, {@code client_id},
 * {@code scope}, {@code jti}, {@code iat}), checked offline against the cached JWKS (refreshed every 5 minutes, and at
 * most once per 10 s on an unknown {@code kid}), then against the revocations of {@code gw.control}.
 */
public final class ApiTokenVerifier {

  private static final Logger log = LoggerFactory.getLogger(ApiTokenVerifier.class);
  static final long REFRESH_MS = 300_000;
  static final long UNKNOWN_KID_MS = 10_000;
  static final long SKEW_MS = 30_000;

  private final JwksSource source;
  private final String issuer;
  private final String audience;
  private final ControlState control;
  private final LongSupplier wallMs;
  private final LongSupplier monoMs;
  private volatile Map<String, ECDSAVerifier> keys = Map.of();
  private volatile boolean loaded;
  private long lastFetchMono = Long.MIN_VALUE / 2;
  private long lastOkMono = Long.MIN_VALUE / 2;

  public ApiTokenVerifier(JwksSource source, String issuer, String audience, ControlState control, LongSupplier wallMs,
      LongSupplier monoMs) {
    this.source = source;
    this.issuer = issuer;
    this.audience = audience;
    this.control = control;
    this.wallMs = wallMs;
    this.monoMs = monoMs;
  }

  /** True once a JWKS was fetched (readiness). */
  public boolean loaded() {
    return loaded;
  }

  /** Fetches now unless one was attempted in the last 10 s; failures keep the cached keys. */
  public synchronized void refresh(boolean force) {
    long now = monoMs.getAsLong();
    if (!force && now - lastFetchMono < UNKNOWN_KID_MS) {
      return;
    }
    lastFetchMono = now;
    try {
      JWKSet set = source.fetch();
      if (set == null) {
        throw new IllegalStateException("no JWKS");
      }
      Map<String, ECDSAVerifier> next = new HashMap<>();
      for (JWK k : set.getKeys()) {
        if (k instanceof ECKey ec && k.getKeyID() != null) {
          next.put(k.getKeyID(), new ECDSAVerifier(ec.toPublicJWK()));
        }
      }
      keys = next;
      loaded = true;
      lastOkMono = now;
    } catch (Exception e) {
      log.warn("JWKS not refreshed, keeping {} keys: {}", keys.size(), e.toString());
    }
  }

  public Caller.ApiCaller verify(String token) {
    if (monoMs.getAsLong() - lastOkMono >= REFRESH_MS) {
      refresh(true);
    }
    SignedJWT jwt;
    JWTClaimsSet c;
    try {
      jwt = SignedJWT.parse(token);
      c = jwt.getJWTClaimsSet();
    } catch (Exception e) {
      throw new InvalidTokenException("malformed token");
    }
    if (!JWSAlgorithm.ES256.equals(jwt.getHeader().getAlgorithm())) {
      throw new InvalidTokenException("alg must be ES256");
    }
    String kid = jwt.getHeader().getKeyID();
    ECDSAVerifier verifier = kid == null ? null : keys.get(kid);
    if (verifier == null && kid != null) {
      refresh(false);
      verifier = keys.get(kid);
    }
    try {
      if (verifier == null || !jwt.verify(verifier)) {
        throw new InvalidTokenException("bad signature or unknown key");
      }
    } catch (InvalidTokenException e) {
      throw e;
    } catch (Exception e) {
      throw new InvalidTokenException("bad signature");
    }
    long now = wallMs.getAsLong();
    if (!issuer.equals(c.getIssuer()) || c.getAudience() == null || !c.getAudience().contains(audience)) {
      throw new InvalidTokenException("wrong issuer or audience");
    }
    Date exp = c.getExpirationTime();
    Date nbf = c.getNotBeforeTime();
    if (exp == null || exp.getTime() + SKEW_MS < now || (nbf != null && nbf.getTime() - SKEW_MS > now)) {
      throw new InvalidTokenException("expired or not yet valid");
    }
    String tenant;
    String clientId;
    String scope;
    try {
      tenant = c.getStringClaim("tenant");
      clientId = c.getStringClaim("client_id");
      scope = c.getStringClaim("scope");
    } catch (Exception e) {
      throw new InvalidTokenException("malformed claims");
    }
    if (tenant == null || tenant.isEmpty() || clientId == null || clientId.isEmpty()) {
      throw new InvalidTokenException("tenant and client_id are required");
    }
    String jti = c.getJWTID();
    if (jti != null && control.jtiRevoked(jti)) {
      throw new RevokedTokenException("token revoked");
    }
    long iat = c.getIssueTime() == null ? 0 : c.getIssueTime().getTime();
    if (control.clientBlocked(clientId, iat)) {
      throw new RevokedTokenException("client disabled");
    }
    Set<String> scopes = new LinkedHashSet<>();
    if (scope != null) {
      for (String s : scope.split(" ")) {
        if (!s.isBlank()) {
          scopes.add(s);
        }
      }
    }
    return new Caller.ApiCaller(tenant, clientId, Set.copyOf(scopes));
  }
}
