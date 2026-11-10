package com.pangate.pushapi.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.SeqKeys;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.pushapi.config.PushApiProperties;
import com.pangate.pushapi.core.StoreUnavailableException;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisStoresIT {

  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static RedisState state;

  @BeforeAll
  static void boot() {
    redis.start();
    state = RedisState.connect(new PushApiProperties.Redis("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
        "standalone", Duration.ofMillis(200)));
  }

  @AfterAll
  static void shutdown() {
    state.close();
    redis.stop();
  }

  @BeforeEach
  void flush() throws Exception {
    state.async().flushall().get();
  }

  @Test
  void seqIncrIsSharedWithTheGatewayKey() throws Exception {
    RedisSeqStore seq = new RedisSeqStore(state);
    assertThat(seq.incr(SeqKeys.seq("t1:u1"))).isEqualTo(1);
    assertThat(seq.incr(SeqKeys.seq("t1:u1"))).isEqualTo(2);
    assertThat(state.async().get("seq:{t1:u1}").get()).isEqualTo("2");
  }

  @Test
  void seqEpochsReadsSlots() throws Exception {
    RedisSeqEpochs epochs = new RedisSeqEpochs(state);
    state.async().hset(SeqKeys.SEQ_EPOCHS, Integer.toString(SeqKeys.slot("t1:u1")), "4").get();
    assertThat(epochs.epoch("t1:u1")).isZero();
    epochs.refreshNow();
    assertThat(epochs.epoch("t1:u1")).isEqualTo(4);
    assertThat(epochs.epoch("t1:other-slot-xyz")).isZero();
  }

  @Test
  void idemSetIfAbsentAndDeleteIfEquals() {
    RedisIdemStore idem = new RedisIdemStore(state);
    assertThat(idem.setIfAbsent("idem:{t1}:k", "pending|a", Duration.ofSeconds(30))).isTrue();
    assertThat(idem.setIfAbsent("idem:{t1}:k", "pending|b", Duration.ofSeconds(30))).isFalse();
    assertThat(idem.get("idem:{t1}:k")).contains("pending|a");
    assertThat(idem.deleteIfEquals("idem:{t1}:k", "pending|b")).isFalse();
    assertThat(idem.deleteIfEquals("idem:{t1}:k", "pending|a")).isTrue();
    assertThat(idem.get("idem:{t1}:k")).isEmpty();
    idem.set("idem:{t1}:k", "done|x", Duration.ofMillis(100));
    assertThat(idem.get("idem:{t1}:k")).contains("done|x");
  }

  @Test
  void statusDeliveredWinsAndOrphansAreNotCreated() throws Exception {
    long now = System.currentTimeMillis();
    RedisMessageStatus status = new RedisMessageStatus(state, System::currentTimeMillis);
    PushRecord r = Fixtures.withMsgId(Fixtures.message("t1:u1"), "M1").toBuilder().setReceipt(true)
        .setAcceptedAtMs(now).setExpiresAtMs(now + 3_600_000).build();
    status.accepted(r);
    assertThat(status.get("t1", "M1")).get().satisfies(m -> {
      assertThat(m).containsEntry("state", "accepted").containsEntry("kind", "message").containsEntry("receipt", "true")
          .containsEntry("target", "t1:u1").containsEntry("class", "reliable");
    });
    long ttl = state.async().pttl("msg:{t1}:M1").get();
    assertThat(ttl).isBetween(7_100_000L, 7_200_000L);   // expires_at − now + 1 h
    status.delivered(DeliveredRecord.newBuilder().setMsgId("M1").setTenantId("t1").setConnId("C1")
        .setStatus(AckItem.Status.STATUS_ACKED).setAtMs(now + 5).build());
    status.offline(OfflineRecord.newBuilder().setRecord(r).setReason(OfflineRecord.Reason.REASON_CLOSED)
        .setAttempts(3).setAtMs(now + 9).build());
    assertThat(status.get("t1", "M1")).get().satisfies(m -> assertThat(m).containsEntry("state", "delivered")
        .containsEntry("deliveredConns", "1").containsEntry("lastStatus", "acked").doesNotContainKey("offlineReason"));

    PushRecord orphan = Fixtures.withMsgId(Fixtures.message("t1:u1"), "M2");
    status.offline(OfflineRecord.newBuilder().setRecord(orphan).setReason(OfflineRecord.Reason.REASON_NO_SESSION)
        .build());
    assertThat(state.async().exists("msg:{t1}:M2").get()).isZero();
    assertThat(status.get("t1", "M2")).isEmpty();
  }

  @Test
  void offlineRecordsReasonAndAttempts() {
    long now = System.currentTimeMillis();
    RedisMessageStatus status = new RedisMessageStatus(state, System::currentTimeMillis);
    PushRecord r = Fixtures.withMsgId(Fixtures.message("t1:u1"), "M3").toBuilder().setAcceptedAtMs(now)
        .setExpiresAtMs(now + 60_000).build();
    status.accepted(r);
    status.offline(OfflineRecord.newBuilder().setRecord(r).setReason(OfflineRecord.Reason.REASON_NO_SESSION)
        .setAttempts(0).setAtMs(now).build());
    assertThat(status.get("t1", "M3")).get().satisfies(m -> assertThat(m).containsEntry("state", "offline")
        .containsEntry("offlineReason", "no_session").containsEntry("attempts", "0"));
  }

  @Test
  void timeoutBecomesStoreUnavailable() {
    RedisSeqStore seq = new RedisSeqStore(state);
    var docker = redis.getDockerClient();
    docker.pauseContainerCmd(redis.getContainerId()).exec();
    try {
      long t0 = System.nanoTime();
      assertThatThrownBy(() -> seq.incr("seq:{t1:u1}")).isInstanceOf(StoreUnavailableException.class);
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(2));
    } finally {
      docker.unpauseContainerCmd(redis.getContainerId()).exec();
    }
  }
}
