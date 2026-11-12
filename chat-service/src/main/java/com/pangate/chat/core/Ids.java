package com.pangate.chat.core;

import java.util.concurrent.ThreadLocalRandom;

/** ULIDs: 48-bit ms time + 80 random bits, Crockford base32 (chat spec §3: conversation, message and client ids). */
public final class Ids {

  private static final char[] A = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();

  private Ids() {}

  public static String next() {
    return next(System.currentTimeMillis());
  }

  public static String next(long nowMs) {
    char[] out = new char[26];
    long t = nowMs;
    for (int i = 9; i >= 0; i--) {
      out[i] = A[(int) (t & 31)];
      t >>>= 5;
    }
    ThreadLocalRandom r = ThreadLocalRandom.current();
    for (int i = 10; i < 26; i++) {
      out[i] = A[r.nextInt(32)];
    }
    return new String(out);
  }

  /** 26 Crockford base32 characters (upper case), first one at most '7' (48-bit time). */
  public static boolean isUlid(String s) {
    if (s == null || s.length() != 26 || s.charAt(0) > '7') {
      return false;
    }
    for (int i = 0; i < 26; i++) {
      if (java.util.Arrays.binarySearch(A, s.charAt(i)) < 0) {
        return false;
      }
    }
    return true;
  }
}
