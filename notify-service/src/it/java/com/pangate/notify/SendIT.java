package com.pangate.notify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.devtools.notify.FakeApns;
import com.pangate.devtools.notify.FakeFcm;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.domain.PushToken;
import com.pangate.notify.support.Containers;
import com.pangate.notify.support.DevCredentialsForTests;
import com.pangate.notify.support.NotifyIT;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Offline notifications spec §5.4, §5.5: a NotifyRequest on Kafka reaches APNs and FCM; dead tokens go. */
@NotifyIT
class SendIT {

  static final FakeApns APNS;
  static final FakeFcm FCM;
  static final Path CREDS = DevCredentialsForTests.dir();
  static final String TOPIC = "it.notify.send";

  static {
    try {
      APNS = FakeApns.start(0);
      FCM = FakeFcm.start(0);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @DynamicPropertySource
  static void providers(DynamicPropertyRegistry r) {
    // its own topic: the cached context of the other ITs (no providers) consumes c1.notify.requests in the same group
    r.add("notify.kafka.requests-topic", () -> TOPIC);
    r.add("notify.tenants.chat.apns.base-url", () -> "http://127.0.0.1:" + APNS.port());
    r.add("notify.tenants.chat.apns.sandbox-base-url", () -> "http://127.0.0.1:" + APNS.port());
    r.add("notify.tenants.chat.apns.key-file", () -> CREDS.resolve("apns.p8").toString());
    r.add("notify.tenants.chat.apns.key-id", () -> "KEY123");
    r.add("notify.tenants.chat.apns.team-id", () -> "TEAM123");
    r.add("notify.tenants.chat.apns.bundle-id", () -> "com.pangate.chat");
    r.add("notify.tenants.chat.fcm.base-url", () -> "http://127.0.0.1:" + FCM.port());
    r.add("notify.tenants.chat.fcm.token-uri", () -> "http://127.0.0.1:" + FCM.port() + "/token");
    r.add("notify.tenants.chat.fcm.project-id", () -> "p1");
    r.add("notify.tenants.chat.fcm.service-account-file", () -> CREDS.resolve("fcm.json").toString());
  }

  @Autowired TokenStore store;
  final HttpClient http = HttpClient.newHttpClient();

  static String hex(String prefix) {
    return (prefix + UUID.randomUUID() + UUID.randomUUID()).replace("-", "").substring(0, 64);
  }

  int received(int port, String token) throws Exception {
    String body = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/received?token="
        + token)).build(), HttpResponse.BodyHandlers.ofString()).body();
    return body.equals("[]") ? 0 : body.split("\"headers\"").length - 1;
  }

  static void produce(NotifyRequest r) throws Exception {
    try (KafkaProducer<String, byte[]> k = new KafkaProducer<>(Map.of("bootstrap.servers",
        Containers.kafka().getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
      k.send(new ProducerRecord<>(TOPIC, r.getPrincipalId(), r.toByteArray())).get();
    }
  }

  static NotifyRequest req(String principal, String id) {
    long now = System.currentTimeMillis();
    return NotifyRequest.newBuilder().setRequestId(id).setTenantId("chat").setPrincipalId(principal)
        .setAlert(Alert.newBuilder().setTitle("Lan").setBody("hi")).setBadge(2).setThreadId("c1").setCollapseId(id)
        .setCreatedAtMs(now).setExpiresAtMs(now + 3_600_000).setHighPriority(true).build();
  }

  @Test
  void requestReachesApnsAndFcmOnceAndDeadTokensGo() throws Exception {
    String p = "chat:" + UUID.randomUUID();
    String apnsToken = hex("a");
    String fcmToken = "fcm-" + UUID.randomUUID();
    String deadToken = hex("dead");
    store.register(new PushToken("chat", p, "d1", Platform.APNS, "production", apnsToken, Instant.now()));
    store.register(new PushToken("chat", p, "d2", Platform.FCM, "production", fcmToken, Instant.now()));
    store.register(new PushToken("chat", p, "d3", Platform.APNS, "sandbox", deadToken, Instant.now()));
    String id = "m-" + UUID.randomUUID() + "/" + p;
    produce(req(p, id));
    await().atMost(Duration.ofSeconds(30)).until(() -> received(APNS.port(), apnsToken) == 1
        && received(FCM.port(), fcmToken) == 1);
    await().atMost(Duration.ofSeconds(10)).until(() -> store.byDevice("chat", p, "d3").isEmpty());
    produce(req(p, id));   // the same request again: deduplicated
    Thread.sleep(3_000);
    assertThat(received(APNS.port(), apnsToken)).isEqualTo(1);
    assertThat(received(FCM.port(), fcmToken)).isEqualTo(1);
  }
}
