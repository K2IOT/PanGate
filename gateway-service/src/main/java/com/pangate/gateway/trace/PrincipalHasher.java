package com.pangate.gateway.trace;

import com.pangate.starter.secrets.SecretSource;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** principalId as it may appear in logs and spans (spec §8): HMAC-SHA256(principal-hash.key), first 8 bytes, hex. */
public final class PrincipalHasher {

  private final SecretKeySpec key;
  // One Mac per thread (Mac is not thread-safe; getInstance per call is a provider lookup).
  private final ThreadLocal<Mac> macs = ThreadLocal.withInitial(this::newMac);

  private PrincipalHasher(byte[] key) {
    this.key = new SecretKeySpec(key, "HmacSHA256");
  }

  public static PrincipalHasher load(SecretSource s) {
    String name = "principal-hash.key";
    try {
      return new PrincipalHasher(Base64.getDecoder().decode(new String(s.get(name), StandardCharsets.US_ASCII).trim()));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Invalid principal hash key " + s.describe(name) + ": not base64", e);
    }
  }

  /** A throwaway key: hashes are still never the raw principal (tests, code paths without secrets). */
  public static PrincipalHasher random() {
    byte[] k = new byte[32];
    new SecureRandom().nextBytes(k);
    return new PrincipalHasher(k);
  }

  private Mac newMac() {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(key);
      return mac;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  public String hash(String principal) {
    byte[] h = macs.get().doFinal(principal.getBytes(StandardCharsets.UTF_8));  // doFinal resets the Mac
    return HexFormat.of().formatHex(h, 0, 8);
  }
}
