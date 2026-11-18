package com.pangate.notify;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.domain.PushToken;
import com.pangate.notify.support.NotifyIT;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Offline notifications spec §5.2: one token per device, a token on one device only. */
@NotifyIT
class PushTokenStoreIT {

  @Autowired TokenStore store;

  static String id() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  static String apns() {
    return (id() + id()).substring(0, 64);
  }

  static PushToken t(String principal, String device, String token, Instant at) {
    return new PushToken("chat", principal, device, Platform.APNS, "production", token, at);
  }

  @Test
  void registerThenLookupByPrincipalAndDevice() {
    String p = "chat:" + id();
    String tok = apns();
    store.register(t(p, "d1", tok, Instant.now()));
    assertThat(store.byPrincipal("chat", p)).extracting(PushToken::token).containsExactly(tok);
    assertThat(store.byDevice("chat", p, "d1")).map(PushToken::token).hasValue(tok);
    assertThat(store.byDevice("other", p, "d1")).isEmpty();
  }

  @Test
  void reRegisterReplacesTheDevicesToken() {
    String p = "chat:" + id();
    store.register(t(p, "d1", apns(), Instant.now()));
    String second = apns();
    store.register(t(p, "d1", second, Instant.now()));
    assertThat(store.byPrincipal("chat", p)).extracting(PushToken::token).containsExactly(second);
  }

  @Test
  void sameTokenMovesToTheNewDevice() {
    String a = "chat:" + id();
    String b = "chat:" + id();
    String phone = apns();
    store.register(t(a, "d1", phone, Instant.now()));
    assertThat(store.register(t(b, "d2", phone, Instant.now()))).isEqualTo(1);
    assertThat(store.byPrincipal("chat", a)).isEmpty();
    assertThat(store.byPrincipal("chat", b)).extracting(PushToken::deviceId).containsExactly("d2");
  }

  @Test
  void removeIfTokenKeepsANewerToken() {
    String p = "chat:" + id();
    String old = apns();
    store.register(t(p, "d1", old, Instant.now()));
    String fresh = apns();
    store.register(t(p, "d1", fresh, Instant.now()));
    assertThat(store.removeIfToken("chat", p, "d1", old)).isFalse();
    assertThat(store.byDevice("chat", p, "d1")).map(PushToken::token).hasValue(fresh);
    assertThat(store.removeIfToken("chat", p, "d1", fresh)).isTrue();
    assertThat(store.byDevice("chat", p, "d1")).isEmpty();
  }

  @Test
  void removeDeviceAcrossTenants() {
    String p = "x:" + id();
    store.register(new PushToken("t1", p, "d1", Platform.APNS, "production", apns(), Instant.now()));
    store.register(new PushToken("t2", p, "d1", Platform.APNS, "production", apns(), Instant.now()));
    store.register(new PushToken("t1", p, "d2", Platform.APNS, "production", apns(), Instant.now()));
    assertThat(store.removeDevice(p, "d1")).isEqualTo(2);
    assertThat(store.byPrincipal("t1", p)).extracting(PushToken::deviceId).containsExactly("d2");
  }

  @Test
  void removePrincipal() {
    String p = "chat:" + id();
    store.register(t(p, "d1", apns(), Instant.now()));
    store.register(t(p, "d2", apns(), Instant.now()));
    assertThat(store.removePrincipal(p)).isEqualTo(2);
    assertThat(store.byPrincipal("chat", p)).isEmpty();
  }

  @Test
  void purgeIdleRemovesOnlyOldRows() {
    String p = "chat:" + id();
    Instant now = Instant.now();
    store.register(t(p, "old", apns(), now.minus(Duration.ofDays(200))));
    store.register(t(p, "new", apns(), now));
    assertThat(store.purgeIdle(now.minus(Duration.ofDays(180)))).isGreaterThanOrEqualTo(1);
    assertThat(store.byPrincipal("chat", p)).extracting(PushToken::deviceId).containsExactly("new");
  }
}
