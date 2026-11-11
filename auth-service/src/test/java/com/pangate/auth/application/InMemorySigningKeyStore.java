package com.pangate.auth.application;

import com.pangate.auth.application.port.SigningKeyStore;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class InMemorySigningKeyStore implements SigningKeyStore {

  final Map<String, SigningKey> rows = new LinkedHashMap<>();

  @Override
  public List<SigningKey> all() {
    return new ArrayList<>(rows.values());
  }

  @Override
  public void insert(SigningKey k) {
    if (k.state() == KeyState.ACTIVE && rows.values().stream().anyMatch(r -> r.state() == KeyState.ACTIVE)) {
      throw new IllegalStateException("one_active_key");
    }
    rows.put(k.kid(), k);
  }

  @Override
  public void update(SigningKey k) {
    rows.put(k.kid(), k);
  }

  @Override
  public void delete(String kid) {
    rows.remove(kid);
  }

  @Override
  public int retireActive(Instant retireAt, Instant removeAt) {
    int n = 0;
    for (SigningKey k : all()) {
      if (k.state() == KeyState.ACTIVE) {
        rows.put(k.kid(), k.withState(KeyState.RETIRED, retireAt, removeAt));
        n++;
      }
    }
    return n;
  }

  List<SigningKey> in(KeyState s) {
    return rows.values().stream().filter(k -> k.state() == s).toList();
  }
}
