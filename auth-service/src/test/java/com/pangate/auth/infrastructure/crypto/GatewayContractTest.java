package com.pangate.auth.infrastructure.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.JWKSet;
import com.pangate.auth.domain.ApiClaims;
import com.pangate.auth.domain.ClaimLimits;
import com.pangate.auth.domain.ConnectClaims;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.domain.SigningKey;
import com.pangate.gateway.auth.JwksCache;
import com.pangate.gateway.auth.JwksSource;
import com.pangate.gateway.auth.JwtResult;
import com.pangate.gateway.auth.JwtVerifier;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Tokens minted by Auth against the gateway's real verifier (spec §10, contract). */
class GatewayContractTest {

  static final String ISS = "https://auth.pangate";

  private final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
  private final SigningKey key = Es256TokenSigner.newKey(now, KeyState.ACTIVE);
  private final Es256TokenSigner signer = new Es256TokenSigner(ISS);
  private final ScheduledExecutorService tick = Executors.newSingleThreadScheduledExecutor();

  @AfterEach
  void stop() {
    tick.shutdownNow();
  }

  private JwtVerifier verifier() {
    JwksSource src = etag -> new JwksSource.Updated(new JWKSet(key.privateJwk().toPublicJWK()), "e1",
        Duration.ofMinutes(5));
    JwksCache cache = new JwksCache(src, tick, Runnable::run, Duration.ofMinutes(5), System::currentTimeMillis);
    cache.refresh().join();
    return new JwtVerifier(cache, "pangate-gw", ISS, Duration.ofSeconds(30));
  }

  @Test
  void gatewayAcceptsAConnectTokenMintedByAuth() {
    var req = ClaimLimits.validate("t1", "t1:u1", "d1", List.of("t1:news.*"), 600);
    String token = signer.sign(key, new ConnectClaims(ISS, req, "jti-1", now, now.plusSeconds(600)));

    JwtResult r = verifier().verify(token, now.getEpochSecond());

    assertThat(r).isInstanceOf(JwtResult.Verified.class);
    var c = ((JwtResult.Verified) r).claims();
    assertThat(c.tenant()).isEqualTo("t1");
    assertThat(c.principal()).isEqualTo("t1:u1");
    assertThat(c.device()).isEqualTo("d1");
    assertThat(c.topics()).containsExactly("t1:news.*");
    assertThat(c.jti()).isEqualTo("jti-1");
    assertThat(c.expSec()).isEqualTo(now.getEpochSecond() + 600);
  }

  @Test
  void gatewayAcceptsAConnectTokenWithoutTopics() {
    var req = ClaimLimits.validate("t1", "t1:u1", "d1", null, null);
    String token = signer.sign(key, new ConnectClaims(ISS, req, "jti-2", now, now.plusSeconds(600)));
    assertThat(verifier().verify(token, now.getEpochSecond())).isInstanceOf(JwtResult.Verified.class);
  }

  @Test
  void gatewayRejectsAnApiToken() {
    String token = signer.sign(key, new ApiClaims(ISS, "t1-backend", "t1", EnumSet.of(Scope.TOKENS_MINT), "j",
        now, now.plusSeconds(900)));
    assertThat(verifier().verify(token, now.getEpochSecond())).isEqualTo(new JwtResult.Rejected("wrong audience"));
  }

  @Test
  void apiTokenRoundTripsThroughVerifyApi() {
    var claims = new ApiClaims(ISS, "t1-backend", "t1", EnumSet.of(Scope.TOKENS_MINT, Scope.PUSH_SEND), "j1",
        now, now.plusSeconds(900));
    String token = signer.sign(key, claims);
    var pub = Optional.of(key.publicKey());

    assertThat(signer.verifyApi(token, kid -> kid.equals(key.kid()) ? pub : Optional.empty(), now)).contains(claims);
    assertThat(signer.verifyApi(token, kid -> Optional.empty(), now)).isEmpty();
    assertThat(signer.verifyApi(token, kid -> pub, now.plusSeconds(931))).isEmpty();      // expired beyond skew
    assertThat(signer.verifyApi(token, kid -> pub, now.plusSeconds(920))).isPresent();    // within 30 s skew
    assertThat(new Es256TokenSigner("https://other").verifyApi(token, kid -> pub, now)).isEmpty();
    var req = ClaimLimits.validate("t1", "t1:u1", "d1", null, null);
    String connect = signer.sign(key, new ConnectClaims(ISS, req, "j2", now, now.plusSeconds(600)));
    assertThat(signer.verifyApi(connect, kid -> pub, now)).isEmpty();                     // wrong audience
    assertThat(signer.verifyApi("garbage", kid -> pub, now)).isEmpty();
  }

  @Test
  void kidIsTheRfc7638Thumbprint() throws Exception {
    assertThat(key.kid()).isEqualTo(key.privateJwk().computeThumbprint().toString());
  }
}
