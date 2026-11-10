package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.PushResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class IdempotencyTest {

  final AtomicLong mono = new AtomicLong();
  /** In-memory store with expiry on the fake clock. */
  final Map<String, String> values = new HashMap<>();
  final Map<String, Long> expiry = new HashMap<>();
  Runnable onSleep = () -> { };
  final IdemStore store = new IdemStore() {
    void expire(String k) {
      Long at = expiry.get(k);
      if (at != null && at <= mono.get()) {
        values.remove(k);
        expiry.remove(k);
      }
    }
    @Override public Optional<String> get(String key) { expire(key); return Optional.ofNullable(values.get(key)); }
    @Override public boolean setIfAbsent(String key, String value, Duration ttl) {
      expire(key);
      if (values.containsKey(key)) {
        return false;
      }
      set(key, value, ttl);
      return true;
    }
    @Override public void set(String key, String value, Duration ttl) {
      values.put(key, value);
      expiry.put(key, mono.get() + ttl.toMillis());
    }
    @Override public boolean deleteIfEquals(String key, String value) {
      expire(key);
      if (value.equals(values.get(key))) {
        values.remove(key);
        return true;
      }
      return false;
    }
  };
  final List<Duration> sleeps = new ArrayList<>();
  final Idempotency idem = new Idempotency(store, Duration.ofHours(1), Duration.ofSeconds(30), Duration.ofSeconds(2),
      d -> {
        sleeps.add(d);
        mono.addAndGet(d.toMillis());
        onSleep.run();
      }, mono::get);
  final AtomicInteger reserves = new AtomicInteger();

  Reserved reserve() {
    reserves.incrementAndGet();
    return new Reserved("M" + reserves.get(), 10 + reserves.get(), 1, 0);
  }

  static PushResponse ok(String msgId) {
    return PushResponse.newBuilder().setOk(true).setCode(PushResponse.Code.CODE_OK).setMsgId(msgId).setSeq(11)
        .setCellId("c1").build();
  }

  @Test
  void firstRequestOwnsTheKey() {
    Idempotency.Claim c = idem.claim("t1", "k", this::reserve);
    assertThat(c).isInstanceOf(Idempotency.Owned.class);
    assertThat(((Idempotency.Owned) c).reserved()).isEqualTo(new Reserved("M1", 11, 1, 0));
    assertThat(values).containsEntry("idem:{t1}:k", "pending|M1|11|1|0").containsEntry("idem:{t1}:k:last",
        "pending|M1|11|1|0");
  }

  @Test
  void doneReturnsTheFirstResponseAsDuplicate() {
    Idempotency.Owned o = (Idempotency.Owned) idem.claim("t1", "k", this::reserve);
    idem.complete(o, ok("M1"));
    Idempotency.Claim again = idem.claim("t1", "k", this::reserve);
    assertThat(again).isInstanceOf(Idempotency.Done.class);
    PushResponse r = ((Idempotency.Done) again).first();
    assertThat(r.getDuplicate()).isTrue();
    assertThat(r.getMsgId()).isEqualTo("M1");
    assertThat(r.getSeq()).isEqualTo(11);
    assertThat(r.getCode()).isEqualTo(PushResponse.Code.CODE_OK);
    assertThat(reserves).hasValue(1);
  }

  @Test
  void pendingWaitsThenBusy() {
    idem.claim("t1", "k", this::reserve);
    assertThat(idem.claim("t1", "k", this::reserve)).isInstanceOf(Idempotency.Busy.class);
    assertThat(sleeps).hasSize(40).allMatch(d -> d.equals(Duration.ofMillis(50)));
  }

  @Test
  void pendingThatFinishesWhileWaitingIsDone() {
    Idempotency.Owned o = (Idempotency.Owned) idem.claim("t1", "k", this::reserve);
    onSleep = () -> {
      if (sleeps.size() == 3) {
        idem.complete(o, ok("M1"));
      }
    };
    assertThat(idem.claim("t1", "k", this::reserve)).isInstanceOf(Idempotency.Done.class);
    assertThat(sleeps).hasSize(3);
  }

  @Test
  void abandonDeletesOnlyOurPending() {
    Idempotency.Owned o = (Idempotency.Owned) idem.claim("t1", "k", this::reserve);
    values.put("idem:{t1}:k", "pending|OTHER|1|0|0");
    idem.abandon(o);
    assertThat(values).containsEntry("idem:{t1}:k", "pending|OTHER|1|0|0");
    values.put("idem:{t1}:k", o.value());
    idem.abandon(o);
    assertThat(values).doesNotContainKey("idem:{t1}:k");
  }

  /** Review Focus 1: a retry after a failed produce, or after a crash, reuses the values first reserved. */
  @Test
  void afterACrashTheRetryReusesTheReservedMsgIdAndSeq() {
    idem.claim("t1", "k", this::reserve);
    mono.addAndGet(30_001);                      // pending expired, :last (1 h) still there
    Idempotency.Claim c = idem.claim("t1", "k", this::reserve);
    assertThat(c).isInstanceOf(Idempotency.Owned.class);
    assertThat(((Idempotency.Owned) c).reserved()).isEqualTo(new Reserved("M1", 11, 1, 0));
    assertThat(reserves).hasValue(1);
  }

  @Test
  void abandonedThenRetriedReusesTheReservedValues() {
    Idempotency.Owned o = (Idempotency.Owned) idem.claim("t1", "k", this::reserve);
    idem.abandon(o);
    Idempotency.Claim c = idem.claim("t1", "k", this::reserve);
    assertThat(((Idempotency.Owned) c).reserved().msgId()).isEqualTo("M1");
    assertThat(reserves).hasValue(1);
  }

  @Test
  void sameKeyDifferentBodyIsStillDuplicate() {
    idem.complete((Idempotency.Owned) idem.claim("t1", "k", this::reserve), ok("M1"));
    assertThat(idem.claim("t1", "k", () -> new Reserved("OTHER", 99, 0, 0))).isInstanceOf(Idempotency.Done.class);
  }

  @Test
  void keysAreScopedByTenant() {
    idem.complete((Idempotency.Owned) idem.claim("t1", "k", this::reserve), ok("M1"));
    assertThat(idem.claim("t2", "k", this::reserve)).isInstanceOf(Idempotency.Owned.class);
  }
}
