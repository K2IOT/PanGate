package com.pangate.notify.application;

import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.PushToken;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory {@link TokenStore}: key {@code tenant|principal|device}. */
public final class FakeTokenStore implements TokenStore {

  public final Map<String, PushToken> rows = new ConcurrentHashMap<>();
  public final List<String> calls = new ArrayList<>();

  static String key(String tenant, String principal, String device) {
    return tenant + "|" + principal + "|" + device;
  }

  @Override
  public synchronized int register(PushToken t) {
    int moved = 0;
    for (var e : List.copyOf(rows.entrySet())) {
      PushToken o = e.getValue();
      if (o.platform() == t.platform() && o.token().equals(t.token())
          && !e.getKey().equals(key(t.tenantId(), t.principalId(), t.deviceId()))) {
        rows.remove(e.getKey());
        moved++;
      }
    }
    rows.put(key(t.tenantId(), t.principalId(), t.deviceId()), t);
    return moved;
  }

  @Override
  public void remove(String tenant, String principal, String device) {
    rows.remove(key(tenant, principal, device));
  }

  @Override
  public List<PushToken> byPrincipal(String tenant, String principal) {
    return rows.values().stream().filter(t -> t.tenantId().equals(tenant) && t.principalId().equals(principal))
        .toList();
  }

  @Override
  public Optional<PushToken> byDevice(String tenant, String principal, String device) {
    return Optional.ofNullable(rows.get(key(tenant, principal, device)));
  }

  @Override
  public synchronized int removeDevice(String principal, String device) {
    calls.add("removeDevice " + principal + "/" + device);
    int n = 0;
    for (var e : List.copyOf(rows.entrySet())) {
      if (e.getValue().principalId().equals(principal) && e.getValue().deviceId().equals(device)) {
        rows.remove(e.getKey());
        n++;
      }
    }
    return n;
  }

  @Override
  public synchronized int removePrincipal(String principal) {
    calls.add("removePrincipal " + principal);
    int n = 0;
    for (var e : List.copyOf(rows.entrySet())) {
      if (e.getValue().principalId().equals(principal)) {
        rows.remove(e.getKey());
        n++;
      }
    }
    return n;
  }

  @Override
  public synchronized boolean removeIfToken(String tenant, String principal, String device, String token) {
    PushToken t = rows.get(key(tenant, principal, device));
    if (t != null && t.token().equals(token)) {
      rows.remove(key(tenant, principal, device));
      return true;
    }
    return false;
  }

  @Override
  public synchronized int purgeIdle(Instant before) {
    calls.add("purgeIdle " + before);
    int n = 0;
    for (var e : List.copyOf(rows.entrySet())) {
      if (e.getValue().updatedAt().isBefore(before)) {
        rows.remove(e.getKey());
        n++;
      }
    }
    return n;
  }
}
