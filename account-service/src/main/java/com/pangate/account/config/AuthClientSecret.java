package com.pangate.account.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.starter.secrets.SecretSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

/**
 * The secret of the auth client account-service uses: from {@code clients.json} written by auth-service's seed
 * (local, dev, ITs), else from the {@link SecretSource} under {@code clientSecretName} (production).
 */
public final class AuthClientSecret {

  private AuthClientSecret() {}

  @SuppressWarnings("unchecked")
  public static String load(AccountProperties.Auth cfg, SecretSource secrets) {
    if (cfg.clientFile() != null) {
      try {
        Map<String, String> clients = new ObjectMapper().readValue(Files.readString(cfg.clientFile()), Map.class);
        String s = clients.get(cfg.clientId());
        if (s == null) {
          throw new IllegalStateException(cfg.clientId() + " is not in " + cfg.clientFile());
        }
        return s;
      } catch (IOException e) {
        throw new IllegalStateException("cannot read " + cfg.clientFile(), e);
      }
    }
    return new String(secrets.get(cfg.clientSecretName()), StandardCharsets.UTF_8).trim();
  }
}
