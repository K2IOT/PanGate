package com.pangate.router.kafka;

import java.util.concurrent.ThreadLocalRandom;

/** ULID: 48-bit ms time + 80 random bits, Crockford base32 (event_id of NODE_DOWN). */
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
    long hi = r.nextLong() >>> 24;
    long lo = r.nextLong() >>> 24;
    for (int i = 17; i >= 10; i--) {
      out[i] = A[(int) (hi & 31)];
      hi >>>= 5;
    }
    for (int i = 25; i >= 18; i--) {
      out[i] = A[(int) (lo & 31)];
      lo >>>= 5;
    }
    return new String(out);
  }
}
