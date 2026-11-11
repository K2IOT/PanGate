package com.pangate.auth.infrastructure.crypto;

import com.pangate.auth.application.port.SecretHasher;
import java.nio.CharBuffer;
import org.springframework.security.crypto.argon2.Argon2PasswordEncoder;

/** Argon2id with the OWASP parameters m=19 MiB, t=2, p=1 (spec §7). */
public final class Argon2SecretHasher implements SecretHasher {

  private final Argon2PasswordEncoder encoder = new Argon2PasswordEncoder(16, 32, 1, 19456, 2);
  private final String dummy = encoder.encode("timing-equalisation-dummy");

  @Override
  public String hash(char[] secret) {
    return encoder.encode(CharBuffer.wrap(secret));
  }

  @Override
  public boolean matches(char[] secret, String hash) {
    return hash != null && encoder.matches(CharBuffer.wrap(secret), hash);
  }

  @Override
  public void burn() {
    encoder.matches("not-the-dummy", dummy);
  }
}
