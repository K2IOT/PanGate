package com.pangate.account.infrastructure.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API tokens of auth-service for the admin API (account spec §5; auth spec §3: ES256, {@code aud=pangate-api},
 * claims {@code tenant}, {@code client_id}, {@code scope}, {@code jti}), checked offline against the JWKS (as
 * push-api's verifier: refreshed every 5 minutes, at most once per 10 s on an unknown kid), then against the jti and
 * client revocations of gw.control.
 */
public final class ApiTokenVerifier {

  public record ApiCaller(String clientId, String tenant, Set<String> scopes) {}

  private static final Logger log = LoggerFactory.getLogger(ApiTokenVerifier.class);
  static final long REFRESH_MS = 300_000;
  static final long UNKNOWN_KID_MS = 10_000;
  static final long SKEW_MS = 30_000;

  private final JwksSource source;
  private final String issuer;
  private final Revocations revocations;
  private final LongSupplier wallMs;
  private final LongSupplier monoMs;
  private volatile Map<String, ECDSAVerifier> keys = Map.of();
  private final ReentrantLock lock = new ReentrantLock();
  private long lastFetchMono = Long.MIN_VALUE / 2;
  private volatile long lastOkMono = Long.MIN_VALUE / 2;

  public ApiTokenVerifier(JwksSource source, String issuer, Revocations revocations, LongSupplier wallMs,
      LongSupplier monoMs) {
    this.source = source;
    this.issuer = issuer;
    this.revocations = revocations;
    this.wallMs = wallMs;
    this.monoMs = monoMs;
  }

  private void fetch(boolean force) {
    long now = monoMs.getAsLong();
    if (!force && now - lastFetchMono < UNKNOWN_KID_MS) {
      return;
    }
    lastFetchMono = now;
    try {
      JWKSet set = source.fetch();
      Map<String, ECDSAVerifier> next = new HashMap<>();
      for (JWK k : set.getKeys()) {
        if (k instanceof ECKey ec && k.getKeyID() != null) {
          next.put(k.getKeyID(), new ECDSAVerifier(ec.toPublicJWK()));
        }
      }
      keys = next;
      lastOkMono = now;
    } catch (Exception e) {
      log.warn("JWKS not refreshed, keeping {} keys: {}", keys.size(), e.toString());
    }
  }

  public ApiCaller verify(String token) {
    boolean stale = monoMs.getAsLong() - lastOkMono >= REFRESH_MS;
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
    if ((stale || (kid != null && !keys.containsKey(kid))) && lock.tryLock()) {
      try {
        fetch(stale);
      } finally {
        lock.unlock();
      }
    }
    ECDSAVerifier verifier = kid == null ? null : keys.get(kid);
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
    if (!issuer.equals(c.getIssuer()) || c.getAudience() == null || !c.getAudience().contains("pangate-api")) {
      throw new InvalidTokenException("wrong issuer or audience");
    }
    Date exp = c.getExpirationTime();
    if (exp == null || exp.getTime() + SKEW_MS < now) {
      throw new InvalidTokenException("expired");
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
    if (tenant == null || clientId == null || c.getJWTID() == null) {
      throw new InvalidTokenException("tenant, client_id and jti are required");
    }
    if (revocations.jtiRevoked(c.getJWTID()) || revocations.clientRevoked(clientId)) {
      throw new RevokedTokenException("token revoked");
    }
    return new ApiCaller(clientId, tenant, scope == null ? Set.of() : Set.of(scope.trim().split(" +")));
  }
}
