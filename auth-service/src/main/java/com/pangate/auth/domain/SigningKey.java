package com.pangate.auth.domain;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.ECKey;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;

/** An ES256 key with its private part (in memory only); {@code retireAt}/{@code removeAt} are null until retired. */
public record SigningKey(String kid, ECKey privateJwk, KeyState state, Instant activateAt, Instant retireAt,
    Instant removeAt) {

  public SigningKey withState(KeyState s, Instant retire, Instant remove) {
    return new SigningKey(kid, privateJwk, s, activateAt, retire, remove);
  }

  public ECPublicKey publicKey() {
    try {
      return privateJwk.toECPublicKey();
    } catch (JOSEException e) {
      throw new IllegalStateException("bad EC key " + kid, e);
    }
  }

  @Override
  public String toString() {  // never print the private key
    return "SigningKey[" + kid + ", " + state + ", activateAt=" + activateAt + ", removeAt=" + removeAt + "]";
  }
}
