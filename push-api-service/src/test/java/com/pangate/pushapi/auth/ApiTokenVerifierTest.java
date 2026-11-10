package com.pangate.pushapi.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.Revoke;
import com.pangate.pushapi.core.Caller;
import com.pangate.pushapi.core.ControlState;
import com.pangate.pushapi.core.TenantPolicy;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ApiTokenVerifierTest {

  static final String ISS = "http://auth:8090";
  final AtomicLong wall = new AtomicLong(1_800_000_000_000L);
  final AtomicLong mono = new AtomicLong();
  final ECKey k1 = gen("k1");
  final ECKey k2 = gen("k2");
  final AtomicReference<JWKSet> published = new AtomicReference<>(new JWKSet(List.of(k1.toPublicJWK())));
  final AtomicInteger fetches = new AtomicInteger();
  final ControlState control = new ControlState("c1", new TenantPolicy(false, true, 1, 1, 1), wall::get);
  final ApiTokenVerifier v = new ApiTokenVerifier(() -> {
    fetches.incrementAndGet();
    return published.get();
  }, ISS, "pangate-api", control, wall::get, mono::get);

  static ECKey gen(String kid) {
    try {
      return new ECKeyGenerator(Curve.P_256).keyID(kid).generate();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  String token(ECKey key, String iss, String aud, long expMs, String jti) throws Exception {
    long now = wall.get();
    JWTClaimsSet c = new JWTClaimsSet.Builder().issuer(iss).audience(aud).subject("client:t1-backend")
        .claim("tenant", "t1").claim("client_id", "t1-backend").claim("scope", "push:send push:broadcast")
        .jwtID(jti).issueTime(new Date(now - 1_000)).notBeforeTime(new Date(now - 1_000))
        .expirationTime(new Date(expMs)).build();
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.getKeyID())
        .type(JOSEObjectType.JWT).build(), c);
    jwt.sign(new ECDSASigner(key));
    return jwt.serialize();
  }

  String good() throws Exception {
    return token(k1, ISS, "pangate-api", wall.get() + 900_000, "J1");
  }

  @Test
  void validTokenGivesCaller() throws Exception {
    Caller.ApiCaller c = v.verify(good());
    assertThat(c.tenant()).isEqualTo("t1");
    assertThat(c.clientId()).isEqualTo("t1-backend");
    assertThat(c.scopes()).containsExactlyInAnyOrder("push:send", "push:broadcast");
    assertThat(v.loaded()).isTrue();
  }

  @Test
  void wrongAudienceIssuerSignatureOrExpiredIs401() throws Exception {
    assertThatThrownBy(() -> v.verify(token(k1, ISS, "pangate-gw", wall.get() + 900_000, "J")))
        .isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify(token(k1, "https://evil", "pangate-api", wall.get() + 900_000, "J")))
        .isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify(token(k1, ISS, "pangate-api", wall.get() - 31_000, "J")))
        .isInstanceOf(InvalidTokenException.class);
    v.verify(token(k1, ISS, "pangate-api", wall.get() - 29_000, "J"));   // 30 s clock skew
    String forged = good();
    String[] p = forged.split("\\.");
    assertThatThrownBy(() -> v.verify(p[0] + "." + p[1] + "." + token(k2, ISS, "pangate-api", wall.get() + 1, "J")
        .split("\\.")[2])).isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify("not-a-jwt")).isInstanceOf(InvalidTokenException.class);
  }

  @Test
  void unknownKidRefetchesAtMostOncePerTenSeconds() throws Exception {
    v.verify(good());
    assertThat(fetches).hasValue(1);
    String withK2 = token(k2, ISS, "pangate-api", wall.get() + 900_000, "J2");
    assertThatThrownBy(() -> v.verify(withK2)).isInstanceOf(InvalidTokenException.class);
    assertThat(fetches).hasValue(1);   // the first fetch was just now
    mono.addAndGet(10_001);
    published.set(new JWKSet(List.of(k1.toPublicJWK(), k2.toPublicJWK())));
    v.verify(withK2);
    assertThat(fetches).hasValue(2);
    assertThatThrownBy(() -> v.verify(token(gen("k3"), ISS, "pangate-api", wall.get() + 900_000, "J3")))
        .isInstanceOf(InvalidTokenException.class);
    assertThat(fetches).hasValue(2);
  }

  @Test
  void keysAreRefreshedEveryFiveMinutes() throws Exception {
    v.verify(good());
    mono.addAndGet(300_001);
    v.verify(good());
    assertThat(fetches).hasValue(2);
  }

  @Test
  void revokedJtiAndBlockedClientAre403() throws Exception {
    control.apply("revoke:j:J1", ControlEvent.newBuilder().setRevoke(Revoke.newBuilder().setJti("J1"))
        .setIssuedAtMs(wall.get()).build().toByteArray());
    assertThatThrownBy(() -> v.verify(good())).isInstanceOf(RevokedTokenException.class);
    String other = token(k1, ISS, "pangate-api", wall.get() + 900_000, "J9");
    v.verify(other);
    control.apply("revoke:c:t1-backend", ControlEvent.newBuilder().setRevoke(Revoke.newBuilder()
        .setClientId("t1-backend")).setIssuedAtMs(wall.get()).build().toByteArray());
    assertThatThrownBy(() -> v.verify(other)).isInstanceOf(RevokedTokenException.class);
  }

  @Test
  void jwksDownKeepsTheCachedKeys() throws Exception {
    v.verify(good());
    published.set(null);
    mono.addAndGet(300_001);
    v.verify(good());
  }

  /** Final review: with auth's JWKS down for over 5 minutes, requests must not each fetch it. */
  @Test
  void jwksDownIsFetchedAtMostOncePerTenSecondsFromRequests() throws Exception {
    v.verify(good());
    published.set(null);
    mono.addAndGet(300_001);
    for (int i = 0; i < 50; i++) {
      v.verify(good());
    }
    assertThat(fetches).hasValue(2);
    mono.addAndGet(9_000);
    v.verify(good());
    assertThat(fetches).hasValue(2);
    mono.addAndGet(1_001);
    v.verify(good());
    assertThat(fetches).hasValue(3);
  }

  /** Final review: a request with known keys never waits behind a slow JWKS fetch. */
  @Test
  void requestsWithKnownKeysDoNotWaitForAFetch() throws Exception {
    java.util.concurrent.CountDownLatch inFetch = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    ApiTokenVerifier slow = new ApiTokenVerifier(() -> {
      if (calls.incrementAndGet() > 1) {
        inFetch.countDown();
        release.await();
      }
      return published.get();
    }, ISS, "pangate-api", control, wall::get, mono::get);
    slow.refresh(true);
    mono.addAndGet(300_001);
    Thread fetching = Thread.ofVirtual().start(() -> slow.refresh(true));
    inFetch.await();
    String t = good();
    try {
      assertThat(java.util.concurrent.CompletableFuture.supplyAsync(() -> slow.verify(t))
          .get(2, java.util.concurrent.TimeUnit.SECONDS).tenant()).isEqualTo("t1");
    } finally {
      release.countDown();
      fetching.join();
    }
  }

  /** Final review minor: jti is a required claim (spec §2); without it a jti revocation could never apply. */
  @Test
  void tokenWithoutJtiIs401() throws Exception {
    assertThatThrownBy(() -> v.verify(token(k1, ISS, "pangate-api", wall.get() + 900_000, null)))
        .isInstanceOf(InvalidTokenException.class);
  }
}
