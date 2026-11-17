package com.pangate.notify.support;

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
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;

/** A JWKS endpoint and connect tokens signed like auth-service's (auth spec §3), once per IT JVM. */
public final class FakeAuth {

  public static final String ISSUER = "http://fake-auth";
  private static ECKey key;
  private static String jwksUri;

  private FakeAuth() {}

  public static synchronized String jwksUri() {
    if (jwksUri == null) {
      try {
        key = new ECKeyGenerator(Curve.P_256).keyID("it-1").generate();
        byte[] body = new JWKSet(key.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
        HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        s.createContext("/jwks.json", x -> {
          x.getResponseHeaders().add("Content-Type", "application/json");
          x.sendResponseHeaders(200, body.length);
          x.getResponseBody().write(body);
          x.close();
        });
        s.start();
        jwksUri = "http://127.0.0.1:" + s.getAddress().getPort() + "/jwks.json";
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return jwksUri;
  }

  /** A connect token of {@code principal} on {@code device}, issued {@code iatMs}, valid 10 minutes. */
  public static String token(String tenant, String principal, String device, long iatMs) {
    jwksUri();
    try {
      JWTClaimsSet c = new JWTClaimsSet.Builder().issuer(ISSUER).audience("pangate-gw").subject(principal)
          .claim("tenant", tenant).claim("device", device).jwtID(UUID.randomUUID().toString())
          .issueTime(new Date(iatMs)).expirationTime(new Date(iatMs + 600_000)).build();
      SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("it-1")
          .type(JOSEObjectType.JWT).build(), c);
      jwt.sign(new ECDSASigner(key));
      return jwt.serialize();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static String token(String tenant, String principal, String device) {
    return token(tenant, principal, device, System.currentTimeMillis() - 1_000);
  }
}
