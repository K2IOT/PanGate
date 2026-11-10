package com.pangate.auth.infrastructure.crypto;

import com.pangate.starter.secrets.SecretNotFoundException;
import com.pangate.starter.secrets.SecretSource;

/** A 32-byte key-encryption key read from secret {@code auth.kek.<id>}. */
public record Kek(String id, byte[] key) {

  public static String secretName(String kekId) {
    return "auth.kek." + kekId;
  }

  public static Kek load(SecretSource secrets, String kekId) {
    String name = secretName(kekId);
    byte[] b;
    try {
      b = secrets.get(name);
    } catch (SecretNotFoundException e) {
      throw new KekException(name, secrets.describe(name), "not found", e);
    }
    if (b.length != 32) {
      throw new KekException(name, secrets.describe(name), "must be exactly 32 bytes, found " + b.length, null);
    }
    return new Kek(kekId, b);
  }

  @Override
  public String toString() {
    return "Kek[" + id + "]";
  }
}
