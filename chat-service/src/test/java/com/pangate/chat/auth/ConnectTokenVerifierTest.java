package com.pangate.chat.auth;

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
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Revoke;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class ConnectTokenVerifierTest {

  static final String ISS = "https://auth.pangate";
  final AtomicLong wall = new AtomicLong(1_800_000_000_000L);
  final AtomicLong mono = new AtomicLong();
  final ECKey k1 = gen("k1");
  final AtomicInteger fetches = new AtomicInteger();
  final Revocations revocations = new Revocations(wall::get);
  final ConnectTokenVerifier v = new ConnectTokenVerifier(() -> {
    fetches.incrementAndGet();
    return new JWKSet(List.of(k1.toPublicJWK()));
  }, ISS, "pangate-gw", revocations, wall::get, mono::get);

  static ECKey gen(String kid) {
    try {
      return new ECKeyGenerator(Curve.P_256).keyID(kid).generate();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  String token(UnaryOperator<JWTClaimsSet.Builder> edit) throws Exception {
    long now = wall.get();
    JWTClaimsSet.Builder b = new JWTClaimsSet.Builder().issuer(ISS).audience("pangate-gw").subject("chat:u1")
        .claim("tenant", "chat").claim("device", "d1").jwtID("J1").issueTime(new Date(now - 1_000))
        .notBeforeTime(new Date(now - 1_000)).expirationTime(new Date(now + 900_000));
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("k1").type(JOSEObjectType.JWT)
        .build(), edit.apply(b).build());
    jwt.sign(new ECDSASigner(k1));
    return jwt.serialize();
  }

  @Test
  void validTokenGivesUserWithoutPrefix() throws Exception {
    assertThat(v.verify(token(b -> b))).isEqualTo(new ConnectTokenVerifier.Caller("u1", "d1", "J1"));
    assertThat(v.loaded()).isTrue();
  }

  @Test
  void wrongAudienceIs401() throws Exception {
    assertThatThrownBy(() -> v.verify(token(b -> b.audience("pangate-api"))))
        .isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify(token(b -> b.issuer("https://evil")))).isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify(token(b -> b.expirationTime(new Date(wall.get() - 60_000)))))
        .isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify("not.a.jwt")).isInstanceOf(InvalidTokenException.class);
  }

  @Test
  void missingJtiIs401() throws Exception {
    assertThatThrownBy(() -> v.verify(token(b -> b.jwtID(null)))).isInstanceOf(InvalidTokenException.class);
  }

  @Test
  void principalWithoutChatPrefixIs401() throws Exception {
    assertThatThrownBy(() -> v.verify(token(b -> b.subject("u1")))).isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify(token(b -> b.subject("chat:")))).isInstanceOf(InvalidTokenException.class);
    assertThatThrownBy(() -> v.verify(token(b -> b.claim("tenant", "t1")))).isInstanceOf(InvalidTokenException.class);
  }

  static byte[] revoke(long issuedAt, Revoke r) {
    return ControlEvent.newBuilder().setIssuedAtMs(issuedAt).setRevoke(r).build().toByteArray();
  }

  @Test
  void revokedDeviceIs403() throws Exception {
    String t = token(b -> b);
    revocations.apply("revoke:d:chat:u1/d1", revoke(wall.get(), Revoke.newBuilder()
        .setDevice(DeviceRef.newBuilder().setPrincipalId("chat:u1").setDeviceId("d1")).build()));
    assertThatThrownBy(() -> v.verify(t)).isInstanceOf(RevokedTokenException.class);
    // another device of the same user is still fine
    assertThat(v.verify(token(b -> b.claim("device", "d2"))).deviceId()).isEqualTo("d2");
    // a token issued after the revocation is fine again
    wall.addAndGet(5_000);
    assertThat(v.verify(token(b -> b)).userId()).isEqualTo("u1");
  }

  @Test
  void revokedJtiOrPrincipalIs403() throws Exception {
    String t = token(b -> b);
    revocations.apply("revoke:j:J1", revoke(wall.get(), Revoke.newBuilder().setJti("J1")
        .setExpiresAtMs(wall.get() + 900_000).build()));
    assertThatThrownBy(() -> v.verify(t)).isInstanceOf(RevokedTokenException.class);
    String other = token(b -> b.jwtID("J2"));
    revocations.apply("revoke:p:chat:u1", revoke(wall.get(), Revoke.newBuilder().setPrincipalId("chat:u1").build()));
    assertThatThrownBy(() -> v.verify(other)).isInstanceOf(RevokedTokenException.class);
    revocations.apply("revoke:p:chat:u1", null);   // tombstone lifts it
    assertThat(v.verify(other).jti()).isEqualTo("J2");
  }
}
