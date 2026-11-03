package com.pangate.gateway.test;

import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.gateway.auth.JwksCache;
import com.pangate.gateway.auth.JwksSource;
import com.pangate.gateway.auth.JwtAuthFlow;
import com.pangate.gateway.auth.JwtVerifier;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.auth.TicketKey;
import com.pangate.gateway.auth.TicketKeyRing;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;

/** Signing keys, a mutable JWKS, and the auth services wired with synchronous offload, for handler tests. */
public final class AuthFixture {

  public final ECKey key;
  public final List<ECKey> published = new ArrayList<>();
  public final JwksCache jwks;
  public final JwtVerifier verifier;
  public final JwtAuthFlow flow;
  public final ResumeTicketService tickets;

  public AuthFixture(LongSupplier nowSec) {
    try {
      key = newKey();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    published.add(key);
    jwks = new JwksCache(etag -> new JwksSource.Updated(
        new JWKSet(published.stream().map(k -> (com.nimbusds.jose.jwk.JWK) k.toPublicJWK()).toList()), null, null),
        Executors.newSingleThreadScheduledExecutor(r -> {
          Thread t = new Thread(r, "test-tick");
          t.setDaemon(true);
          return t;
        }), Runnable::run, Duration.ofMinutes(5), () -> nowSec.getAsLong() * 1000);
    jwks.refresh().join();
    verifier = new JwtVerifier(jwks, "pangate-gw", "", Duration.ofSeconds(30));
    flow = new JwtAuthFlow(verifier, jwks, Duration.ofSeconds(2));
    byte[] k = new byte[32];
    tickets = new ResumeTicketService(new TicketKeyRing(new TicketKey(1, k), new TicketKey(2, k), new TicketKey(3, k)));
  }

  public static ECKey newKey() throws Exception {
    return new ECKeyGenerator(Curve.P_256).keyIDFromThumbprint(true).generate();
  }

  public String jwt(String principal, String device, long nowSec) {
    return jwt(key, principal, device, nowSec);
  }

  public String jwt(ECKey signer, String principal, String device, long nowSec) {
    String tenant = principal.substring(0, principal.indexOf(':'));
    return new JwtMinter(signer, "test").mint(
        new TokenRequest(tenant, principal, device, List.of("t1:news.*"), Duration.ofMinutes(10)),
        Instant.ofEpochSecond(nowSec));
  }
}
