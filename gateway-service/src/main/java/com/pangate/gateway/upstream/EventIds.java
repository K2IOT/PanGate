package com.pangate.gateway.upstream;

import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

/** ULID event ids for lifecycle events (push contracts spec §1.2): 48-bit ms time + 80 random bits, Crockford base32. */
public final class EventIds {

  private static final char[] ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();

  private EventIds() {}

  public static String next() {
    return next(System.currentTimeMillis(), ThreadLocalRandom.current());
  }

  public static String next(long nowMs, RandomGenerator rnd) {
    char[] out = new char[26];
    long t = nowMs;
    for (int i = 9; i >= 0; i--) {
      out[i] = ALPHABET[(int) (t & 31)];
      t >>>= 5;
    }
    long hi = rnd.nextLong() >>> 24;  // 40 bits
    long lo = rnd.nextLong() >>> 24;  // 40 bits
    for (int i = 17; i >= 10; i--) {
      out[i] = ALPHABET[(int) (hi & 31)];
      hi >>>= 5;
    }
    for (int i = 25; i >= 18; i--) {
      out[i] = ALPHABET[(int) (lo & 31)];
      lo >>>= 5;
    }
    return new String(out);
  }
}
