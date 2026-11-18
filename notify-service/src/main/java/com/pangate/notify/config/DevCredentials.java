package com.pangate.notify.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.notify.infrastructure.Pem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Throwaway credentials for the devenv and tests (offline notifications spec §7): an APNs .p8 key (EC P-256, PKCS#8
 * PEM) and an FCM service account file (RSA 2048). The fake providers accept any; real keys come from Apple/Google.
 */
public final class DevCredentials {

  private DevCredentials() {}

  public static void apnsKey(Path file) throws Exception {
    KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
    g.initialize(new ECGenParameterSpec("secp256r1"));
    write(file, Pem.encode(g.generateKeyPair().getPrivate().getEncoded()));
  }

  public static void fcmServiceAccount(Path file, String tokenUri) throws Exception {
    KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
    g.initialize(2048);
    Map<String, String> sa = new LinkedHashMap<>();
    sa.put("type", "service_account");
    sa.put("project_id", "pangate-dev");
    sa.put("client_email", "notify@pangate-dev.iam.gserviceaccount.com");
    sa.put("private_key", Pem.encode(g.generateKeyPair().getPrivate().getEncoded()));
    sa.put("token_uri", tokenUri);
    write(file, new ObjectMapper().writeValueAsString(sa));
  }

  private static void write(Path file, String content) throws Exception {
    if (file.getParent() != null) {
      Files.createDirectories(file.getParent());
    }
    Files.writeString(file, content);
  }
}
