package com.pangate.gateway.session;

import java.util.random.RandomGenerator;

/**
 * Self-routing connection id (spec §3.6): 128 bits {@code ts48 | loopIdx8 | sessionKey48 | rand24} encoded as
 * 26 Crockford base32 characters (ULID-like, sortable by time). The gateway decodes the owning loop and session
 * key from the id without any global index.
 */
public final class ConnId {

  private static final char[] ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();
  private static final int[] DECODE = new int[128];
  private static final int LENGTH = 26;

  static {
    java.util.Arrays.fill(DECODE, -1);
    for (int i = 0; i < ALPHABET.length; i++) {
      DECODE[ALPHABET[i]] = i;
    }
  }

  private ConnId() {}

  public static String next(long nowMillis, int loopIdx, long sessionKey, RandomGenerator rnd) {
    long hi = ((nowMillis & 0xFFFF_FFFF_FFFFL) << 16) | ((long) (loopIdx & 0xFF) << 8) | ((sessionKey >>> 40) & 0xFF);
    long lo = ((sessionKey & 0xFF_FFFF_FFFFL) << 24) | (rnd.nextInt() & 0xFF_FFFFL);
    char[] out = new char[LENGTH];
    for (int i = LENGTH - 1; i >= 0; i--) {
      out[i] = ALPHABET[(int) (lo & 31)];
      lo = (lo >>> 5) | (hi << 59);
      hi >>>= 5;
    }
    return new String(out);
  }

  public static int loopIdx(String id) {
    return (int) ((decode(id)[0] >>> 8) & 0xFF);
  }

  public static long sessionKey(String id) {
    long[] v = decode(id);
    return ((v[0] & 0xFF) << 40) | (v[1] >>> 24);
  }

  public static boolean isValid(String id) {
    if (id == null || id.length() != LENGTH) {
      return false;
    }
    for (int i = 0; i < LENGTH; i++) {
      char c = id.charAt(i);
      if (c >= 128 || DECODE[c] < 0) {
        return false;
      }
    }
    return DECODE[id.charAt(0)] < 8;
  }

  private static long[] decode(String id) {
    if (!isValid(id)) {
      throw new IllegalArgumentException("Invalid connId: " + id);
    }
    long hi = 0;
    long lo = 0;
    for (int i = 0; i < LENGTH; i++) {
      int v = DECODE[id.charAt(i)];
      hi = (hi << 5) | (lo >>> 59);
      lo = (lo << 5) | v;
    }
    return new long[] {hi, lo};
  }
}
