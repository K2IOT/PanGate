package com.pangate.auth.application;

import static com.pangate.auth.domain.ClaimLimitsTest.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.application.port.Outbox;
import com.pangate.auth.application.port.RevocationStore;
import com.pangate.auth.domain.Caller;
import com.pangate.auth.domain.Revocation;
import com.pangate.auth.domain.Scope;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RevokeTest {

  static final class Store implements RevocationStore {
    final Map<String, Revocation> byKey = new LinkedHashMap<>();
    final AtomicLong ids = new AtomicLong();

    @Override
    public boolean isJtiRevoked(String jti) {
      return byKey.containsKey("revoke:j:" + jti);
    }

    @Override
    public Revocation upsert(Revocation r) {
      Revocation existing = byKey.get(r.controlKey());
      Revocation out = new Revocation(existing == null ? ids.incrementAndGet() : existing.id(), r.tenantId(),
          r.kind(), r.controlKey(), r.expiresAt(), r.createdBy());
      byKey.put(r.controlKey(), out);
      return out;
    }

    @Override
    public List<Revocation> expired(Instant now, int limit) {
      return byKey.values().stream().filter(r -> !r.expiresAt().isAfter(now)).limit(limit).toList();
    }

    @Override
    public void delete(long id) {
      byKey.values().removeIf(r -> r.id() == id);
    }
  }

  record Row(String topic, String key, byte[] payload, String tenant) {}

  static final class Box implements Outbox {
    final List<Row> rows = new ArrayList<>();

    @Override
    public void append(String topic, String key, byte[] payloadOrNull, String tenantId) {
      rows.add(new Row(topic, key, payloadOrNull, tenantId));
    }

    @Override
    public List<OutboxRow> unsent(int limit) {
      return List.of();
    }

    @Override
    public void markSent(long id) {}

    @Override
    public int deleteSentBefore(Instant t) {
      return 0;
    }
  }

  private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-10T00:00:00Z"));
  private final Store store = new Store();
  private final Box outbox = new Box();
  private final Revoke revoke = new Revoke(store, outbox, (r, at) -> new byte[] {1}, Runnable::run, now::get,
      Duration.ofHours(24), "gw.control");
  private final Caller t1 = new Caller("t1-backend", "t1", Set.of(Scope.SESSIONS_REVOKE), "j");

  @Test
  void principalOutsideCallerTenantIs403() {
    assertThat(assertCode(() -> revoke.principal(t1, "t2:u"), "principal_outside_tenant").httpStatus())
        .isEqualTo(403);
    assertCode(() -> revoke.device(t1, "t2:u", "d"), "principal_outside_tenant");
    assertCode(() -> revoke.principal(new Caller("x", "t1", Set.of(), "j"), "t1:u"), "missing_scope");
    assertCode(() -> revoke.jti(t1, ""), "jti_invalid");
    assertCode(() -> revoke.device(t1, "t1:u", ""), "device_invalid");
  }

  @Test
  void writesRevocationAndOutboxWithTheControlKey() {
    Revocation r = revoke.device(t1, "t1:u1", "d1");
    assertThat(r.controlKey()).isEqualTo("revoke:d:t1:u1/d1");
    assertThat(r.expiresAt()).isEqualTo(now.get().plus(Duration.ofHours(24)));
    assertThat(r.createdBy()).isEqualTo("t1-backend");
    assertThat(outbox.rows).singleElement().satisfies(row -> {
      assertThat(row.topic()).isEqualTo("gw.control");
      assertThat(row.key()).isEqualTo("revoke:d:t1:u1/d1");
      assertThat(row.payload()).isNotNull();
      assertThat(row.tenant()).isEqualTo("t1");
    });
  }

  @Test
  void reRevokeExtendsTheSameRow() {
    Revocation first = revoke.principal(t1, "t1:u1");
    now.set(now.get().plus(Duration.ofHours(3)));
    Revocation second = revoke.principal(t1, "t1:u1");
    assertThat(second.id()).isEqualTo(first.id());
    assertThat(second.expiresAt()).isAfter(first.expiresAt());
    assertThat(store.byKey).hasSize(1);
    assertThat(outbox.rows).hasSize(2);
  }

  @Test
  void expiryTombstonesAndDeletes() {
    revoke.jti(t1, "abc");
    var expire = new ExpireRevocations(store, outbox, Runnable::run, now::get);
    assertThat(expire.runOnce()).isZero();
    now.set(now.get().plus(Duration.ofHours(24)));
    assertThat(expire.runOnce()).isEqualTo(1);
    assertThat(store.byKey).isEmpty();
    assertThat(outbox.rows.getLast().key()).isEqualTo("revoke:j:abc");
    assertThat(outbox.rows.getLast().payload()).isNull();
  }

  @Test
  void expiryDrainsMoreThanOneBatchPerPass() {
    for (int i = 0; i < 1_200; i++) {
      revoke.jti(t1, "j" + i);
    }
    now.set(now.get().plus(Duration.ofHours(25)));
    assertThat(new ExpireRevocations(store, outbox, Runnable::run, now::get).runOnce()).isEqualTo(1_200);
    assertThat(store.byKey).isEmpty();
  }

  @Test
  void deviceRevocationRejectsSlash() {
    assertCode(() -> revoke.device(t1, "t1:u", "a/b"), "device_invalid");
  }
}
