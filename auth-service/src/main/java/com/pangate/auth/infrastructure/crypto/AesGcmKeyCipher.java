package com.pangate.auth.infrastructure.crypto;

import com.pangate.auth.application.port.KeyCipher;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** AES-256-GCM with a random 12-byte nonce and the kid as AAD (spec §3). */
public final class AesGcmKeyCipher implements KeyCipher {

  private static final int NONCE = 12;
  private static final int TAG_BITS = 128;

  private final Map<String, Kek> keks;
  private final String current;
  private final SecureRandom random = new SecureRandom();

  public AesGcmKeyCipher(Map<String, Kek> keks, String currentKekId) {
    if (!keks.containsKey(currentKekId)) {
      throw new IllegalArgumentException("current KEK " + currentKekId + " is not loaded");
    }
    this.keks = Map.copyOf(keks);
    this.current = currentKekId;
  }

  @Override
  public byte[] encrypt(String kid, byte[] plain) {
    byte[] nonce = new byte[NONCE];
    random.nextBytes(nonce);
    try {
      Cipher c = cipher(Cipher.ENCRYPT_MODE, keks.get(current), nonce, kid);
      byte[] ct = c.doFinal(plain);
      return ByteBuffer.allocate(NONCE + ct.length).put(nonce).put(ct).array();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("cannot seal key " + kid, e);
    }
  }

  @Override
  public byte[] decrypt(String kekId, String kid, byte[] sealed) {
    Kek kek = keks.get(kekId);
    if (kek == null) {
      throw new IllegalStateException("KEK " + kekId + " is not loaded (secret " + Kek.secretName(kekId) + ")");
    }
    if (sealed.length <= NONCE) {
      throw new IllegalStateException("sealed key " + kid + " is too short");
    }
    try {
      Cipher c = cipher(Cipher.DECRYPT_MODE, kek, Arrays.copyOf(sealed, NONCE), kid);
      return c.doFinal(sealed, NONCE, sealed.length - NONCE);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("cannot open key " + kid + " with KEK " + kekId, e);
    }
  }

  @Override
  public String currentKekId() {
    return current;
  }

  private static Cipher cipher(int mode, Kek kek, byte[] nonce, String kid) throws GeneralSecurityException {
    Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
    c.init(mode, new SecretKeySpec(kek.key(), "AES"), new GCMParameterSpec(TAG_BITS, nonce));
    c.updateAAD(kid.getBytes(StandardCharsets.UTF_8));
    return c;
  }
}
