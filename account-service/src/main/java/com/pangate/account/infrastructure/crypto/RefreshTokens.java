package com.pangate.account.infrastructure.crypto;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.regex.Pattern;

/**
 * Refresh tokens (account spec §2): {@code rt_} + base64url of 32 random bytes. Only SHA-256 of the token is stored;
 * that is enough for 256 random bits.
 */
public final class RefreshTokens {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final Pattern SHAPE = Pattern.compile("^rt_[A-Za-z0-9_-]{43}$");

  private RefreshTokens() {}

  public static String newToken() {
    byte[] b = new byte[32];
    RANDOM.nextBytes(b);
    return "rt_" + Base64.getUrlEncoder().withoutPadding().encodeToString(b);
  }

  public static boolean looksValid(String token) {
    return token != null && SHAPE.matcher(token).matches();
  }

  public static byte[] hash(String token) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.US_ASCII));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
