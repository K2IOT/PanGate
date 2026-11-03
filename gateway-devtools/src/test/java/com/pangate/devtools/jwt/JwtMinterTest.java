package com.pangate.devtools.jwt;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.devtools.keys.DevKeys;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JwtMinterTest {

  @Test
  void mintedTokenHasRequiredClaims(@TempDir Path d) throws Exception {
    ECKey key = DevKeys.loadOrCreateEs256(d.resolve("k.pem"));
    Instant now = Instant.ofEpochSecond(1_762_000_000L);

    String jwt = new JwtMinter(key, "pangate-devtools")
        .mint(new TokenRequest("t1", "t1:u1", "d1", List.of("t1:news.*"), Duration.ofMinutes(10)), now);

    SignedJWT parsed = SignedJWT.parse(jwt);
    assertThat(parsed.verify(new ECDSAVerifier(key.toPublicJWK()))).isTrue();
    assertThat(parsed.getHeader().getAlgorithm()).isEqualTo(JWSAlgorithm.ES256);
    assertThat(parsed.getHeader().getKeyID()).isEqualTo(key.getKeyID());
    JWTClaimsSet c = parsed.getJWTClaimsSet();
    assertThat(c.getAudience()).containsExactly("pangate-gw");
    assertThat(c.getIssuer()).isEqualTo("pangate-devtools");
    assertThat(c.getSubject()).isEqualTo("t1:u1");
    assertThat(c.getStringClaim("tenant")).isEqualTo("t1");
    assertThat(c.getStringClaim("device")).isEqualTo("d1");
    assertThat(c.getStringListClaim("topics")).containsExactly("t1:news.*");
    assertThat(c.getJWTID()).isNotBlank();
    assertThat(c.getNotBeforeTime().toInstant()).isEqualTo(now);
    assertThat(c.getExpirationTime().toInstant()).isEqualTo(now.plusSeconds(600));
  }

  @Test
  void eachTokenHasDistinctJti(@TempDir Path d) throws Exception {
    JwtMinter minter = new JwtMinter(DevKeys.loadOrCreateEs256(d.resolve("k.pem")), "i");
    TokenRequest req = new TokenRequest("t1", "t1:u1", "d1", List.of(), Duration.ofMinutes(1));

    String a = SignedJWT.parse(minter.mint(req, Instant.now())).getJWTClaimsSet().getJWTID();
    String b = SignedJWT.parse(minter.mint(req, Instant.now())).getJWTClaimsSet().getJWTID();

    assertThat(a).isNotEqualTo(b);
  }
}
