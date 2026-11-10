package com.pangate.auth.application.port;

/** Seals signing-key private parts with a KEK (AES-256-GCM, AAD = kid). */
public interface KeyCipher {

  /** Seals with {@link #currentKekId()}; layout {@code nonce(12) || ciphertext || tag(16)}. */
  byte[] encrypt(String kid, byte[] plain);

  /** @throws IllegalStateException if the KEK is unknown or authentication fails */
  byte[] decrypt(String kekId, String kid, byte[] sealed);

  String currentKekId();
}
