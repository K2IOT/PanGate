package com.pangate.starter.secrets;

/** Read-only access to named secrets (TLS keys, ticket keys, hash keys). */
public interface SecretSource {

  /**
   * Returns the secret's bytes.
   *
   * @throws SecretNotFoundException if the secret does not exist
   */
  byte[] get(String name);

  /** Human-readable location of the secret, used in error messages. */
  String describe(String name);
}
