package com.pangate.pushapi.core;

import java.util.concurrent.ThreadLocalRandom;

/** ULID: 48-bit ms time + 80 random bits, Crockford base32 (msg_id, push contracts §1.1). */
public final class Ulids {

  private static final char[] A = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();

  private Ulids() {}

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
}
