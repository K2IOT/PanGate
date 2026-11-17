package com.pangate.devtools.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.devtools.notify.FakeApns;
import com.pangate.devtools.notify.FakeFcm;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Offline notifications spec §7: APNs and FCM stand-ins that record what they got and play dead tokens. */
class FakeProvidersTest {

  final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
  FakeApns apns;
  FakeFcm fcm;

  @AfterEach
  void stop() {
    if (apns != null) {
      apns.close();
    }
    if (fcm != null) {
      fcm.close();
    }
  }

  static String apnsJwt() throws Exception {
    ECKey k = new ECKeyGenerator(Curve.P_256).keyID("KEY123").generate();
    SignedJWT j = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("KEY123").build(),
        new JWTClaimsSet.Builder().issuer("TEAM123").issueTime(new Date()).build());
    j.sign(new ECDSASigner(k));
    return j.serialize();
  }

  HttpResponse<String> post(String url, Map<String, String> headers, String body) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString(body));
    headers.forEach(b::header);
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  List<Object> received(String base, String token) throws Exception {
    String body = http.send(HttpRequest.newBuilder(URI.create(base + "/received?token=" + token)).build(),
        HttpResponse.BodyHandlers.ofString()).body();
    return JSONObjectUtils.getJSONArray(JSONObjectUtils.parse("{\"r\":" + body + "}"), "r");
  }

  @Test
  void apnsRecordsAndPlaysDeadAndBusyTokens() throws Exception {
    apns = FakeApns.start(0);
    String base = "http://127.0.0.1:" + apns.port();
    Map<String, String> h = Map.of("authorization", "bearer " + apnsJwt(), "apns-topic", "com.pangate.chat",
        "apns-push-type", "alert", "apns-collapse-id", "m1");
    String body = "{\"aps\":{\"alert\":{\"title\":\"Lan\",\"body\":\"hi\"},\"badge\":3}}";
    assertThat(post(base + "/3/device/aa11", h, body).statusCode()).isEqualTo(200);
    List<Object> got = received(base, "aa11");
    assertThat(got).hasSize(1);
    assertThat(got.get(0).toString()).contains("hi").contains("m1");

    HttpResponse<String> dead = post(base + "/3/device/dead1", h, body);
    assertThat(dead.statusCode()).isEqualTo(410);
    assertThat(dead.body()).contains("Unregistered");

    HttpResponse<String> busy = post(base + "/3/device/busy1", h, body);
    assertThat(busy.statusCode()).isEqualTo(429);
    assertThat(busy.headers().firstValue("retry-after")).hasValue("1");
    assertThat(post(base + "/3/device/busy1", h, body).statusCode()).isEqualTo(200);

    assertThat(post(base + "/3/device/aa11", Map.of("apns-topic", "x"), body).statusCode()).isEqualTo(403);
  }

  @Test
  void fcmIssuesATokenThenRecordsAndPlaysDeadTokens() throws Exception {
    fcm = FakeFcm.start(0);
    String base = "http://127.0.0.1:" + fcm.port();
    HttpResponse<String> tok = post(base + "/token", Map.of("Content-Type", "application/x-www-form-urlencoded"),
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=x.y.z");
    assertThat(tok.statusCode()).isEqualTo(200);
    assertThat(tok.body()).contains("\"access_token\":\"fake\"");

    Map<String, String> h = Map.of("Authorization", "Bearer fake", "Content-Type", "application/json");
    String ok = "{\"message\":{\"token\":\"t1\",\"notification\":{\"title\":\"Lan\",\"body\":\"hi\"}}}";
    HttpResponse<String> sent = post(base + "/v1/projects/p1/messages:send", h, ok);
    assertThat(sent.statusCode()).isEqualTo(200);
    assertThat(sent.body()).contains("projects/p1/messages/");
    assertThat(received(base, "t1")).hasSize(1);

    HttpResponse<String> dead = post(base + "/v1/projects/p1/messages:send", h,
        "{\"message\":{\"token\":\"dead2\",\"notification\":{\"body\":\"x\"}}}");
    assertThat(dead.statusCode()).isEqualTo(404);
    assertThat(dead.body()).contains("UNREGISTERED");

    assertThat(post(base + "/v1/projects/p1/messages:send", Map.of("Content-Type", "application/json"), ok)
        .statusCode()).isEqualTo(401);
  }
}
