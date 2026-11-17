package com.pangate.account.infrastructure.crypto;

import com.pangate.account.application.port.PasswordHasher;
import java.nio.CharBuffer;
import org.springframework.security.crypto.argon2.Argon2PasswordEncoder;

/** Argon2id with the OWASP parameters m=19 MiB, t=2, p=1 (account spec §7), as auth-service hashes secrets. */
public final class Argon2PasswordHasher implements PasswordHasher {

  private final Argon2PasswordEncoder encoder = new Argon2PasswordEncoder(16, 32, 1, 19456, 2);
  private final String dummy = encoder.encode("timing-equalisation-dummy");

  @Override
  public String hash(char[] password) {
    return encoder.encode(CharBuffer.wrap(password));
  }

  @Override
  public boolean matches(char[] password, String hash) {
    return hash != null && encoder.matches(CharBuffer.wrap(password), hash);
  }

  @Override
  public void burn() {
    encoder.matches("not-the-dummy", dummy);
  }
}
