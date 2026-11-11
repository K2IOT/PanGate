package com.pangate.pushapi.support;

import static org.awaitility.Awaitility.await;

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
import com.pangate.pushapi.PushApiApplication;
import com.pangate.pushapi.kafka.ControlListener;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** A running Push API (profile local) against the given Kafka and Redis, with a JWKS served from this JVM. */
public final class PushApiHarness implements AutoCloseable {

  public static final String ISSUER = "http://localhost:8090";
  public final ECKey key;
  public final int http;
  public final int mgmt;
  public final int grpc;
  public final ConfigurableApplicationContext ctx;
  private final HttpServer jwks;
  private final HttpClient client = HttpClient.newHttpClient();

  public PushApiHarness(String kafka, String redisUri, String... extra) throws Exception {
    key = new ECKeyGenerator(Curve.P_256).keyID("it-key").generate();
    jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    byte[] body = new JWKSet(List.of(key.toPublicJWK())).toString().getBytes(StandardCharsets.UTF_8);
    jwks.createContext("/jwks", ex -> {
      ex.sendResponseHeaders(200, body.length);
      ex.getResponseBody().write(body);
      ex.close();
    });
    jwks.start();
    http = freePort();
    mgmt = freePort();
    grpc = freePort();
    List<String> args = new ArrayList<>(List.of("--spring.config.name=push-api", "--server.port=" + http,
        "--push-api.management-port=" + mgmt, "--push-api.grpc.port=" + grpc,
        "--push-api.kafka.bootstrap=" + kafka, "--push-api.redis.uri=" + redisUri,
        "--push-api.auth.jwks-uri=http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks",
        "--push-api.auth.issuer=" + ISSUER, "--push-api.kafka.status-group=it-" + UUID.randomUUID()));
    args.addAll(List.of(extra));
    ctx = new SpringApplicationBuilder(PushApiApplication.class).profiles("local").run(args.toArray(String[]::new));
    await().atMost(Duration.ofSeconds(60)).until(() -> ctx.getBean(ControlListener.class).caughtUp());
  }

  public static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }

  public String token(String tenant, String clientId, String scope) throws Exception {
    return token(tenant, clientId, scope, UUID.randomUUID().toString(), System.currentTimeMillis() - 1_000);
  }

  public String token(String tenant, String clientId, String scope, String jti, long iatMs) throws Exception {
    JWTClaimsSet c = new JWTClaimsSet.Builder().issuer(ISSUER).audience("pangate-api").subject("client:" + clientId)
        .claim("tenant", tenant).claim("client_id", clientId).claim("scope", scope).jwtID(jti)
        .issueTime(new Date(iatMs)).notBeforeTime(new Date(iatMs)).expirationTime(new Date(iatMs + 900_000)).build();
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.getKeyID())
        .type(JOSEObjectType.JWT).build(), c);
    jwt.sign(new ECDSASigner(key));
    return jwt.serialize();
  }

  public HttpResponse<String> post(int port, String path, String token, String json, String... headers)
      throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
        .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    for (int i = 0; i + 1 < headers.length; i += 2) {
      b.header(headers[i], headers[i + 1]);
    }
    return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> get(int port, String path, String token) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET();
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  @Override
  public void close() {
    ctx.close();
    jwks.stop(0);
  }
}
