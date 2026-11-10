package com.pangate.auth.infrastructure.crypto;

import com.pangate.starter.secrets.SecretSource;

/** A 32-byte key-encryption key read from secret {@code auth.kek.<id>}. */
public record Kek(String id, byte[] key) {

  public static String secretName(String kekId) {
    return "auth.kek." + kekId;
  }

  public static Kek load(SecretSource secrets, String kekId) {
    String name = secretName(kekId);
    byte[] b = secrets.get(name);
    if (b.length != 32) {
      throw new IllegalStateException("Secret " + name + " at " + secrets.describe(name) + " must be exactly 32 bytes, "
          + "found " + b.length);
    }
    return new Kek(kekId, b);
  }

  @Override
  public String toString() {
    return "Kek[" + id + "]";
  }
}
