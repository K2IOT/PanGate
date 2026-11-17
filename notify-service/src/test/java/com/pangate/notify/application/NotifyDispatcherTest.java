package com.pangate.notify.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.notify.application.port.Dedup;
import com.pangate.notify.application.port.PushProvider;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.domain.PushToken;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** Offline notifications spec §5.4: one request, every token of the recipient, the provider's answers. */
class NotifyDispatcherTest {

  static final long NOW = 1_800_000_000_000L;

  final FakeTokenStore store = new FakeTokenStore();
  final Set<String> seen = new HashSet<>();
  boolean dedupDown;
  final Dedup dedup = id -> {
    if (dedupDown) {
      throw new IllegalStateException("redis down");
    }
    return seen.add(id);
  };
  final List<String> sent = new ArrayList<>();
  final Map<String, Deque<SendResult>> script = new HashMap<>();
  final PushProvider provider = (t, r) -> {
    sent.add(t.token());
    Deque<SendResult> q = script.get(t.token());
    return CompletableFuture.completedFuture(q == null || q.isEmpty() ? new SendResult.Ok() : q.poll());
  };
  final List<Long> waits = new ArrayList<>();
  final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  final Set<String> configured = new HashSet<>(Set.of("chat/APNS", "chat/FCM"));
  final NotifyDispatcher d = new NotifyDispatcher(store, dedup,
      (tenant, platform) -> configured.contains(tenant + "/" + platform) ? provider : null,
      ms -> {
        waits.add(ms);
        return CompletableFuture.completedFuture(null);
      }, meters, 3);

  void token(String principal, String device, Platform p, String token) {
    store.register(new PushToken("chat", principal, device, p, "production", token, Instant.now()));
  }

  static NotifyRequest req(String id) {
    return NotifyRequest.newBuilder().setRequestId(id).setTenantId("chat").setPrincipalId("chat:b")
        .setAlert(Alert.newBuilder().setTitle("Lan").setBody("hi")).setBadge(1).setCreatedAtMs(NOW)
        .setExpiresAtMs(NOW + 3_600_000).build();
  }

  String handle(NotifyRequest r) {
    return d.handle(r, NOW).join();
  }

  @Test
  void sendsToEveryTokenOfThePrincipal() {
    token("chat:b", "d1", Platform.APNS, "t1");
    token("chat:b", "d2", Platform.FCM, "t2");
    assertThat(handle(req("r1"))).isEqualTo("sent");
    assertThat(sent).containsExactlyInAnyOrder("t1", "t2");
  }

  @Test
  void deviceTargetSendsToOneDevice() {
    token("chat:b", "d1", Platform.APNS, "t1");
    token("chat:b", "d2", Platform.APNS, "t2");
    handle(req("r1").toBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId("chat:b").setDeviceId("d2")).build());
    assertThat(sent).containsExactly("t2");
  }

  @Test
  void expiredIsDropped() {
    token("chat:b", "d1", Platform.APNS, "t1");
    assertThat(handle(req("r1").toBuilder().setExpiresAtMs(NOW - 1).setCreatedAtMs(NOW - 10).build()))
        .isEqualTo("expired");
    assertThat(sent).isEmpty();
  }

  @Test
  void invalidIsDropped() {
    assertThat(handle(req("r1").toBuilder().clearRequestId().build())).isEqualTo("invalid");
  }

  @Test
  void duplicateRequestIsSentOnce() {
    token("chat:b", "d1", Platform.APNS, "t1");
    assertThat(handle(req("r1"))).isEqualTo("sent");
    assertThat(handle(req("r1"))).isEqualTo("duplicate");
    assertThat(sent).containsExactly("t1");
  }

  @Test
  void dedupDownStillSends() {
    token("chat:b", "d1", Platform.APNS, "t1");
    dedupDown = true;
    assertThat(handle(req("r1"))).isEqualTo("sent");
    assertThat(sent).containsExactly("t1");
  }

  @Test
  void noTokenIsCounted() {
    assertThat(handle(req("r1"))).isEqualTo("no_token");
    assertThat(meters.counter("notify.requests", "result", "no_token").count()).isEqualTo(1);
  }

  @Test
  void tenantWithoutConfigIsCounted() {
    configured.clear();
    token("chat:b", "d1", Platform.APNS, "t1");
    assertThat(handle(req("r1"))).isEqualTo("no_config");
    assertThat(sent).isEmpty();
  }

  @Test
  void deadTokenIsRemoved() {
    token("chat:b", "d1", Platform.APNS, "t1");
    script.put("t1", new ArrayDeque<>(List.of(new SendResult.TokenDead())));
    handle(req("r1"));
    assertThat(store.byDevice("chat", "chat:b", "d1")).isEmpty();
    assertThat(meters.counter("notify.tokens.removed", "reason", "provider").count()).isEqualTo(1);
  }

  @Test
  void deadTokenOnlyRemovedIfStillCurrent() {
    token("chat:b", "d1", Platform.APNS, "t1");
    PushProvider racing = (t, r) -> {
      token("chat:b", "d1", Platform.APNS, "t-new");   // the app registered a new token meanwhile
      return CompletableFuture.completedFuture(new SendResult.TokenDead());
    };
    NotifyDispatcher d2 = new NotifyDispatcher(store, dedup, (tenant, p) -> racing,
        ms -> CompletableFuture.completedFuture(null), meters, 3);
    d2.handle(req("r1"), NOW).join();
    assertThat(store.byDevice("chat", "chat:b", "d1")).map(PushToken::token).hasValue("t-new");
  }

  @Test
  void transientRetriedHonoringRetryAfter() {
    token("chat:b", "d1", Platform.APNS, "t1");
    script.put("t1", new ArrayDeque<>(List.of(new SendResult.Transient(0), new SendResult.Transient(2_500),
        new SendResult.Ok())));
    handle(req("r1"));
    assertThat(sent).containsExactly("t1", "t1", "t1");
    assertThat(waits).containsExactly(1_000L, 2_500L);
  }

  @Test
  void transientGivesUpAfterThreeAttempts() {
    token("chat:b", "d1", Platform.APNS, "t1");
    script.put("t1", new ArrayDeque<>(List.of(new SendResult.Transient(0), new SendResult.Transient(0),
        new SendResult.Transient(0), new SendResult.Ok())));
    handle(req("r1"));
    assertThat(sent).hasSize(3);
    assertThat(waits).containsExactly(1_000L, 4_000L);
    assertThat(meters.counter("notify.sends", "platform", "apns", "result", "failed").count()).isEqualTo(1);
  }

  @Test
  void retryNeverPastExpiry() {
    token("chat:b", "d1", Platform.APNS, "t1");
    script.put("t1", new ArrayDeque<>(List.of(new SendResult.Transient(0), new SendResult.Ok())));
    d.handle(req("r1").toBuilder().setExpiresAtMs(NOW + 500).build(), NOW).join();
    assertThat(sent).containsExactly("t1");
    assertThat(waits).isEmpty();
  }

  @Test
  void authFailedAndRejectedAreNotRetried() {
    token("chat:b", "d1", Platform.APNS, "t1");
    token("chat:b", "d2", Platform.FCM, "t2");
    script.put("t1", new ArrayDeque<>(List.of(new SendResult.AuthFailed())));
    script.put("t2", new ArrayDeque<>(List.of(new SendResult.Rejected("BadTopic"))));
    handle(req("r1"));
    assertThat(sent).containsExactlyInAnyOrder("t1", "t2");
    assertThat(waits).isEmpty();
    assertThat(meters.counter("notify.provider.auth_failed", "platform", "apns").count()).isEqualTo(1);
  }
}
