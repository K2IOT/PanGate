package com.pangate.starter.logging;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** HMAC-SHA256 pseudonym for principal ids in logs, metrics and traces (first 16 hex chars). */
public final class PrincipalHasher {

  private final SecretKeySpec key;

  public PrincipalHasher(byte[] key) {
    this.key = new SecretKeySpec(key.clone(), "HmacSHA256");
  }

  public String hash(String principalId) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(key);
      byte[] digest = mac.doFinal(principalId.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest, 0, 8);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("HmacSHA256 unavailable", e);
    }
  }
}
