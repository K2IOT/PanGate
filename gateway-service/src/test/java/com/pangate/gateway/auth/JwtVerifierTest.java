package com.pangate.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JwtVerifierTest {

  private static final long NOW = 1_762_000_000L;
  private ECKey key;
  private JwtVerifier verifier;

  @BeforeEach
  void setUp() throws Exception {
    key = new ECKeyGenerator(Curve.P_256).keyIDFromThumbprint(true).generate();
    JWKSet set = new JWKSet(key.toPublicJWK());
    JwksCache cache = new JwksCache(etag -> new JwksSource.Updated(set, null, null),
        Executors.newSingleThreadScheduledExecutor(), Runnable::run, Duration.ofMinutes(5), () -> NOW * 1000);
    cache.refresh().join();
    verifier = new JwtVerifier(cache, "pangate-gw", "", Duration.ofSeconds(30));
  }

  private String mint(UnaryOperator<JWTClaimsSet.Builder> edit) throws Exception {
    JWTClaimsSet.Builder b = new JWTClaimsSet.Builder().issuer("i").audience("pangate-gw").subject("t1:u1")
        .claim("tenant", "t1").claim("device", "d1").claim("topics", List.of("t1:news.*")).jwtID("j1")
        .notBeforeTime(new Date(NOW * 1000)).expirationTime(new Date((NOW + 600) * 1000));
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.getKeyID()).build(),
        edit.apply(b).build());
    jwt.sign(new ECDSASigner(key));
    return jwt.serialize();
  }

  @Test
  void validTokenVerified() {
    String jwt = new JwtMinter(key, "pangate-devtools").mint(
        new TokenRequest("t1", "t1:u1", "d1", List.of("t1:a.*"), Duration.ofMinutes(10)), Instant.ofEpochSecond(NOW));

    JwtResult r = verifier.verify(jwt, NOW);

    assertThat(r).isInstanceOfSatisfying(JwtResult.Verified.class, v -> {
      assertThat(v.claims().tenant()).isEqualTo("t1");
      assertThat(v.claims().principal()).isEqualTo("t1:u1");
      assertThat(v.claims().device()).isEqualTo("d1");
      assertThat(v.claims().topics()).containsExactly("t1:a.*");
      assertThat(v.claims().jti()).isNotBlank();
      assertThat(v.claims().expSec()).isEqualTo(NOW + 600);
    });
  }

  @Test
  void wrongAudienceRejected() throws Exception {
    assertThat(verifier.verify(mint(b -> b.audience("other")), NOW)).isInstanceOf(JwtResult.Rejected.class);
  }

  @Test
  void clockSkewWithin30sAccepted() throws Exception {
    assertThat(verifier.verify(mint(b -> b.expirationTime(new Date((NOW - 20) * 1000))), NOW))
        .isInstanceOf(JwtResult.Verified.class);
    assertThat(verifier.verify(mint(b -> b.notBeforeTime(new Date((NOW + 20) * 1000))), NOW))
        .isInstanceOf(JwtResult.Verified.class);
  }

  @Test
  void clockSkewBeyond30sRejected() throws Exception {
    assertThat(verifier.verify(mint(b -> b.expirationTime(new Date((NOW - 31) * 1000))), NOW))
        .isInstanceOf(JwtResult.Rejected.class);
    assertThat(verifier.verify(mint(b -> b.notBeforeTime(new Date((NOW + 31) * 1000))), NOW))
        .isInstanceOf(JwtResult.Rejected.class);
  }

  @Test
  void hs256AndNoneRejected() throws Exception {
    SignedJWT hs = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.HS256).keyID(key.getKeyID()).build(),
        SignedJWT.parse(mint(b -> b)).getJWTClaimsSet());
    hs.sign(new MACSigner(new byte[32]));
    String none = new PlainJWT(SignedJWT.parse(mint(b -> b)).getJWTClaimsSet()).serialize();

    assertThat(verifier.verify(hs.serialize(), NOW)).isInstanceOf(JwtResult.Rejected.class);
    assertThat(verifier.verify(none, NOW)).isInstanceOf(JwtResult.Rejected.class);
    assertThat(verifier.verify("not.a.jwt", NOW)).isInstanceOf(JwtResult.Rejected.class);
  }

  @Test
  void unknownKidReported() throws Exception {
    ECKey other = new ECKeyGenerator(Curve.P_256).keyID("other").generate();
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("other").build(),
        SignedJWT.parse(mint(b -> b)).getJWTClaimsSet());
    jwt.sign(new ECDSASigner(other));

    assertThat(verifier.verify(jwt.serialize(), NOW)).isEqualTo(new JwtResult.UnknownKid("other"));
  }

  @Test
  void badSignatureRejected() throws Exception {
    ECKey other = new ECKeyGenerator(Curve.P_256).generate();
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.getKeyID()).build(),
        SignedJWT.parse(mint(b -> b)).getJWTClaimsSet());
    jwt.sign(new ECDSASigner(other));

    assertThat(verifier.verify(jwt.serialize(), NOW)).isInstanceOf(JwtResult.Rejected.class);
  }

  @Test
  void missingDeviceRejected() throws Exception {
    assertThat(verifier.verify(mint(b -> b.claim("device", null)), NOW)).isInstanceOf(JwtResult.Rejected.class);
    assertThat(verifier.verify(mint(b -> b.jwtID(null)), NOW)).isInstanceOf(JwtResult.Rejected.class);
  }

  @Test
  void subjectOutsideTenantRejected() throws Exception {
    assertThat(verifier.verify(mint(b -> b.subject("t2:u1")), NOW)).isInstanceOf(JwtResult.Rejected.class);
  }

  @Test
  void overLongClaimsRejected() throws Exception {
    assertThat(verifier.verify(mint(b -> b.subject("t1:" + "u".repeat(254))), NOW)).isInstanceOf(JwtResult.Rejected.class);
    assertThat(verifier.verify(mint(b -> b.claim("device", "d".repeat(129))), NOW)).isInstanceOf(JwtResult.Rejected.class);
    String tenant = "t".repeat(65);
    assertThat(verifier.verify(mint(b -> b.claim("tenant", tenant).subject(tenant + ":u")), NOW))
        .isInstanceOf(JwtResult.Rejected.class);
    assertThat(verifier.verify(mint(b -> b.subject("t1:" + "u".repeat(253))), NOW)).isInstanceOf(JwtResult.Verified.class);
  }

  @Test
  void tooManyTopicPatternsRejected() throws Exception {
    assertThat(verifier.verify(mint(b -> b.claim("topics", Collections.nCopies(9, "t1:x"))), NOW))
        .isInstanceOf(JwtResult.Rejected.class);
  }

  @Test
  void issuerCheckedOnlyWhenConfigured() throws Exception {
    JwtVerifier strict = new JwtVerifier(verifierCache(), "pangate-gw", "auth", Duration.ofSeconds(30));

    assertThat(strict.verify(mint(b -> b.issuer("evil")), NOW)).isInstanceOf(JwtResult.Rejected.class);
    assertThat(strict.verify(mint(b -> b.issuer("auth")), NOW)).isInstanceOf(JwtResult.Verified.class);
  }

  private JwksCache verifierCache() {
    JWKSet set = new JWKSet(key.toPublicJWK());
    JwksCache c = new JwksCache(etag -> new JwksSource.Updated(set, null, null),
        Executors.newSingleThreadScheduledExecutor(), Runnable::run, Duration.ofMinutes(5), () -> NOW * 1000);
    c.refresh().join();
    return c;
  }
}
