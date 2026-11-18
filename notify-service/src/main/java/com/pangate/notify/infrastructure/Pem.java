package com.pangate.notify.infrastructure;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/** A PKCS#8 private key in PEM ({@code -----BEGIN PRIVATE KEY-----}), as Apple's .p8 and Google's service accounts. */
public final class Pem {

  private Pem() {}

  public static PrivateKey privateKey(String pem, String algorithm) {
    String b64 = pem.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
        .replaceAll("\\s", "");
    try {
      return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder()
          .decode(b64)));
    } catch (Exception e) {
      throw new IllegalArgumentException("not a PKCS#8 " + algorithm + " private key", e);
    }
  }

  public static String encode(byte[] pkcs8) {
    return "-----BEGIN PRIVATE KEY-----\n" + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(pkcs8)
        + "\n-----END PRIVATE KEY-----\n";
  }
}
