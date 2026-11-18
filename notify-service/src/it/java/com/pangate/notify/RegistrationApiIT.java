package com.pangate.notify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Revoke;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.PushToken;
import com.pangate.notify.support.Containers;
import com.pangate.notify.support.FakeAuth;
import com.pangate.notify.support.NotifyIT;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;

/** Offline notifications spec §5.1: the device registers its own token with its connect token. */
@NotifyIT
class RegistrationApiIT {

  @LocalServerPort int port;
  @Autowired TokenStore store;
  final HttpClient http = HttpClient.newHttpClient();

  static String hex64() {
    return (UUID.randomUUID().toString() + UUID.randomUUID()).replace("-", "").substring(0, 64);
  }

  HttpResponse<String> call(String method, String token, String body) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/devices/self/push"))
        .header("Content-Type", "application/json")
        .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  static String apnsBody(String token) {
    return "{\"platform\":\"apns\",\"token\":\"" + token + "\",\"environment\":\"sandbox\"}";
  }

  @Test
  void registerStoresTheTokenOfTheCallersDevice() throws Exception {
    String p = "chat:" + UUID.randomUUID();
    String tok = hex64();
    HttpResponse<String> r = call("PUT", FakeAuth.token("chat", p, "d1"), apnsBody(tok));
    assertThat(r.statusCode()).as(r.body()).isEqualTo(204);
    PushToken stored = store.byDevice("chat", p, "d1").orElseThrow();
    assertThat(stored.token()).isEqualTo(tok);
    assertThat(stored.environment()).isEqualTo("sandbox");
  }

  @Test
  void badBodyIs400() throws Exception {
    String t = FakeAuth.token("chat", "chat:" + UUID.randomUUID(), "d1");
    HttpResponse<String> r = call("PUT", t, "{\"platform\":\"apns\",\"token\":\"nothex\"}");
    assertThat(r.statusCode()).isEqualTo(400);
    assertThat(r.body()).contains("\"code\":\"invalid_request\"");
    assertThat(call("PUT", t, "{\"platform\":\"pigeon\",\"token\":\"x\"}").statusCode()).isEqualTo(400);
  }

  @Test
  void noTokenIs401() throws Exception {
    HttpResponse<String> r = call("PUT", null, apnsBody(hex64()));
    assertThat(r.statusCode()).isEqualTo(401);
    assertThat(r.body()).contains("\"code\":\"invalid_token\"");
  }

  @Test
  void eleventhRegistrationInAMinuteIs429() throws Exception {
    String t = FakeAuth.token("chat", "chat:" + UUID.randomUUID(), "d1");
    for (int i = 0; i < 10; i++) {
      assertThat(call("PUT", t, apnsBody(hex64())).statusCode()).isEqualTo(204);
    }
    HttpResponse<String> r = call("PUT", t, apnsBody(hex64()));
    assertThat(r.statusCode()).isEqualTo(429);
    assertThat(r.body()).contains("\"code\":\"rate_limited\"");
  }

  @Test
  void deleteIs204EvenWithoutAToken() throws Exception {
    String p = "chat:" + UUID.randomUUID();
    String t = FakeAuth.token("chat", p, "d1");
    assertThat(call("DELETE", t, null).statusCode()).isEqualTo(204);
    call("PUT", t, apnsBody(hex64()));
    assertThat(call("DELETE", t, null).statusCode()).isEqualTo(204);
    assertThat(store.byDevice("chat", p, "d1")).isEmpty();
  }

  @Test
  void revokedDeviceCannotRegister() throws Exception {
    String p = "chat:" + UUID.randomUUID();
    String t = FakeAuth.token("chat", p, "d9", System.currentTimeMillis() - 5_000);
    ControlEvent e = ControlEvent.newBuilder().setIssuedAtMs(System.currentTimeMillis())
        .setRevoke(Revoke.newBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId(p).setDeviceId("d9"))).build();
    try (KafkaProducer<String, byte[]> k = new KafkaProducer<>(Map.of("bootstrap.servers",
        Containers.kafka().getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
      k.send(new ProducerRecord<>("gw.control", "revoke:d:" + p + "/d9", e.toByteArray())).get();
    }
    await().atMost(Duration.ofSeconds(20)).until(() -> call("PUT", t, apnsBody(hex64())).statusCode() == 401);
    assertThat(store.byDevice("chat", p, "d9")).isEmpty();
  }
}
