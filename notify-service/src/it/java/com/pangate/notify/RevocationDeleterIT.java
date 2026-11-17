package com.pangate.notify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Revoke;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.domain.PushToken;
import com.pangate.notify.support.Containers;
import com.pangate.notify.support.NotifyIT;
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

/** Offline notifications spec §5.3: revocations on gw.control remove the tokens (invariant 2). */
@NotifyIT
class RevocationDeleterIT {

  @Autowired TokenStore store;

  static String hex() {
    return (UUID.randomUUID().toString() + UUID.randomUUID()).replace("-", "").substring(0, 64);
  }

  static void produce(String key, byte[] value) throws Exception {
    try (KafkaProducer<String, byte[]> k = new KafkaProducer<>(Map.of("bootstrap.servers",
        Containers.kafka().getBootstrapServers()), new StringSerializer(), new ByteArraySerializer())) {
      k.send(new ProducerRecord<>("gw.control", key, value)).get();
    }
  }

  @Test
  void deviceThenPrincipalRevocationRemoveTokens() throws Exception {
    String p = "chat:" + UUID.randomUUID();
    store.register(new PushToken("chat", p, "d1", Platform.APNS, "production", hex(), Instant.now()));
    store.register(new PushToken("chat", p, "d2", Platform.APNS, "production", hex(), Instant.now()));
    produce("junk", new byte[] {1, 2, 3});   // malformed: skipped, the loop goes on
    produce("revoke:d:" + p + "/d1", ControlEvent.newBuilder().setIssuedAtMs(System.currentTimeMillis())
        .setRevoke(Revoke.newBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId(p).setDeviceId("d1")))
        .build().toByteArray());
    await().atMost(Duration.ofSeconds(20)).until(() -> store.byDevice("chat", p, "d1").isEmpty());
    assertThat(store.byDevice("chat", p, "d2")).isPresent();
    produce("revoke:p:" + p, ControlEvent.newBuilder().setIssuedAtMs(System.currentTimeMillis())
        .setRevoke(Revoke.newBuilder().setPrincipalId(p)).build().toByteArray());
    await().atMost(Duration.ofSeconds(20)).until(() -> store.byPrincipal("chat", p).isEmpty());
  }

  @Test
  void aTokenRegisteredAfterAnOldRevocationStays() throws Exception {
    // the group's offset is committed: a restart does not replay old revocations onto tokens registered since
    String p = "chat:" + UUID.randomUUID();
    produce("revoke:d:" + p + "/d1", ControlEvent.newBuilder().setIssuedAtMs(System.currentTimeMillis())
        .setRevoke(Revoke.newBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId(p).setDeviceId("d1")))
        .build().toByteArray());
    Thread.sleep(3_000);
    store.register(new PushToken("chat", p, "d1", Platform.APNS, "production", hex(), Instant.now()));
    Thread.sleep(2_000);
    assertThat(store.byDevice("chat", p, "d1")).isPresent();
  }
}
