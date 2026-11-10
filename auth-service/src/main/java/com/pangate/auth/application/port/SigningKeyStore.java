package com.pangate.auth.application.port;

import com.nimbusds.jose.jwk.ECKey;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import java.time.Instant;
import java.util.List;

/** Persistent signing keys. Rows whose private key cannot be decrypted are left out of {@link #all()}. */
public interface SigningKeyStore {

  /** A stored key as published: its public part (column public_jwk) and whether this pod can decrypt it. */
  record PublishedKey(String kid, KeyState state, ECKey publicJwk, String kekId, boolean decryptable) {}

  /** {@code usable}: keys this pod can sign with; {@code published}: every stored key (spec §3: all are on JWKS). */
  record Snapshot(List<SigningKey> usable, List<PublishedKey> published) {}

  List<SigningKey> all();

  /** Default for stores where every key is decryptable. */
  default Snapshot load() {
    List<SigningKey> keys = all();
    return new Snapshot(keys, keys.stream().map(k -> new PublishedKey(k.kid(), k.state(), k.privateJwk().toPublicJWK(),
        "", true)).toList());
  }

  void insert(SigningKey k);

  /** Updates state, retire_at and remove_at. */
  void update(SigningKey k);

  void delete(String kid);

  /** Retires every row in state active, including rows {@link #all()} hides; returns how many. */
  int retireActive(Instant retireAt, Instant removeAt);
}
