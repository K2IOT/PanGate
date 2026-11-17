package com.pangate.account.domain;

import java.nio.CharBuffer;
import java.text.Normalizer;
import java.util.Locale;

/**
 * Passwords (account spec §4.1): 8..128 characters (code points) after NFC, not equal (ignoring case) to the
 * username, email or phone. Passwords are hashed and compared in NFC, so é typed either way is one password.
 */
public final class PasswordPolicy {

  public static final int MIN = 8;
  public static final int MAX = 128;

  private PasswordPolicy() {}

  /** NFC form of a password, for hashing or comparing. */
  public static char[] normalise(char[] password) {
    return Normalizer.normalize(CharBuffer.wrap(password), Normalizer.Form.NFC).toCharArray();
  }

  /** The NFC password; {@code email} and {@code phone} may be null. */
  public static char[] check(char[] password, Username username, Email email, Phone phone) {
    char[] nfc = normalise(password == null ? new char[0] : password);
    String s = new String(nfc);
    int n = s.codePointCount(0, s.length());
    if (n < MIN || n > MAX) {
      throw new DomainException("weak_password", "a password is " + MIN + ".." + MAX + " characters");
    }
    String lower = s.toLowerCase(Locale.ROOT);
    if ((username != null && lower.equals(username.key())) || (email != null && lower.equals(email.value()))
        || (phone != null && s.equals(phone.value()))) {
      throw new DomainException("weak_password", "the password must not be your username, email or phone");
    }
    return nfc;
  }
}
