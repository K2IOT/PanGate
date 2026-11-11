package com.pangate.auth.application.port;

/** One-way hashing of client secrets. */
public interface SecretHasher {

  String hash(char[] secret);

  /** False for a null hash. */
  boolean matches(char[] secret, String hash);

  /** Spends the time of one {@link #matches} so an unknown client is not distinguishable by latency. */
  void burn();
}
