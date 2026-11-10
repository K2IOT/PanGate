package com.pangate.auth.application.port;

import com.pangate.auth.domain.SigningKey;
import java.time.Instant;
import java.util.List;

/** Persistent signing keys. Rows whose private key cannot be decrypted are left out of {@link #all()}. */
public interface SigningKeyStore {

  List<SigningKey> all();

  void insert(SigningKey k);

  /** Updates state, retire_at and remove_at. */
  void update(SigningKey k);

  void delete(String kid);

  /** Retires every row in state active, including rows {@link #all()} hides; returns how many. */
  int retireActive(Instant retireAt, Instant removeAt);
}
