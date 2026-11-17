package com.pangate.account.application.port;

/** Password hashing (account spec §7); passwords arrive already in NFC ({@code PasswordPolicy.normalise}). */
public interface PasswordHasher {

  String hash(char[] password);

  boolean matches(char[] password, String hash);

  /** Hashes a dummy so that a login without an account takes as long as one with a wrong password. */
  void burn();
}
