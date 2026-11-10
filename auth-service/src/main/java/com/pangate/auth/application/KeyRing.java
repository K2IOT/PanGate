package com.pangate.auth.application;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.pangate.auth.application.port.SigningKeyStore;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import java.security.interfaces.ECPublicKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory view of the signing keys: the active key for signing, public keys by kid, and the JWKS document. Served
 * from memory so JWKS and token verification keep working while PostgreSQL is down (spec §0, §6).
 */
public final class KeyRing {

  private static final Logger log = LoggerFactory.getLogger(KeyRing.class);

  private record Snapshot(SigningKey active, Map<String, ECPublicKey> byKid, Map<String, KeyState> states,
      String jwks) {}

  private final SigningKeyStore store;
  private volatile Snapshot snap = new Snapshot(null, Map.of(), Map.of(), "{\"keys\":[]}");

  public KeyRing(SigningKeyStore store) {
    this.store = store;
  }

  /** Re-reads the store; on failure keeps serving the previous keys. */
  public void reload() {
    SigningKeyStore.Snapshot s;
    try {
      s = store.load();
    } catch (RuntimeException e) {
      log.warn("signing keys not reloaded, serving the cached set: {}", e.toString());
      return;
    }
    SigningKey active = s.usable().stream().filter(k -> k.state() == KeyState.ACTIVE).findFirst().orElse(null);
    Map<String, ECPublicKey> byKid = new java.util.HashMap<>();
    Map<String, KeyState> states = new java.util.HashMap<>();
    List<JWK> pub = new java.util.ArrayList<>();
    for (SigningKeyStore.PublishedKey k : s.published()) {
      try {
        byKid.put(k.kid(), k.publicJwk().toECPublicKey());
        states.put(k.kid(), k.state());
        pub.add(k.publicJwk().toPublicJWK());
      } catch (com.nimbusds.jose.JOSEException e) {
        log.error("signing key {} has an invalid public key and is not published", k.kid());
      }
    }
    snap = new Snapshot(active, Map.copyOf(byKid), Map.copyOf(states), new JWKSet(pub).toString());
  }

  /** @throws NoSigningKeyException if no key can sign */
  public SigningKey active() {
    SigningKey k = snap.active;
    if (k == null) {
      throw new NoSigningKeyException();
    }
    return k;
  }

  public boolean canSign() {
    return snap.active != null;
  }

  /** Every published key, including keys this pod cannot decrypt (spec §3). */
  public Optional<ECPublicKey> publicKey(String kid) {
    return Optional.ofNullable(snap.byKid.get(kid));
  }

  public String jwksJson() {
    return snap.jwks;
  }

  public long count(com.pangate.auth.domain.KeyState state) {
    return snap.states.values().stream().filter(s -> s == state).count();
  }

  public int size() {
    return snap.byKid.size();
  }
}
