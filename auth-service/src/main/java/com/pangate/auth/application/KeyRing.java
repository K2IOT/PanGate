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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory view of the signing keys: the active key for signing, public keys by kid, and the JWKS document. Served
 * from memory so JWKS and token verification keep working while PostgreSQL is down (spec §0, §6).
 */
public final class KeyRing {

  private static final Logger log = LoggerFactory.getLogger(KeyRing.class);

  private record Snapshot(SigningKey active, Map<String, SigningKey> byKid, String jwks) {}

  private final SigningKeyStore store;
  private volatile Snapshot snap = new Snapshot(null, Map.of(), "{\"keys\":[]}");

  public KeyRing(SigningKeyStore store) {
    this.store = store;
  }

  /** Re-reads the store; on failure keeps serving the previous keys. */
  public void reload() {
    List<SigningKey> keys;
    try {
      keys = store.all();
    } catch (RuntimeException e) {
      log.warn("signing keys not reloaded, serving the cached set: {}", e.toString());
      return;
    }
    SigningKey active = keys.stream().filter(k -> k.state() == KeyState.ACTIVE).findFirst().orElse(null);
    Map<String, SigningKey> byKid = keys.stream().collect(Collectors.toUnmodifiableMap(SigningKey::kid,
        Function.identity()));
    List<JWK> pub = keys.stream().map(k -> (JWK) k.privateJwk().toPublicJWK()).toList();
    snap = new Snapshot(active, byKid, new JWKSet(pub).toString());
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

  public Optional<ECPublicKey> publicKey(String kid) {
    SigningKey k = snap.byKid.get(kid);
    return k == null ? Optional.empty() : Optional.of(k.publicKey());
  }

  public String jwksJson() {
    return snap.jwks;
  }

  public long count(com.pangate.auth.domain.KeyState state) {
    return snap.byKid.values().stream().filter(k -> k.state() == state).count();
  }

  public int size() {
    return snap.byKid.size();
  }
}
