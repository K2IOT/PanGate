package com.pangate.gateway.shed;

/**
 * "Number of HANDSHAKING sessions rising continuously for 5 s" (spec §7.3, L3): one sample per second, rising when
 * the last six samples strictly increase. Single-threaded (gw-tick).
 */
public final class HandshakeTrend {

  private static final int SAMPLES = 6;

  private final int[] ring = new int[SAMPLES];
  private int count;
  private int next;
  private long lastSecond = Long.MIN_VALUE;

  /** Called every 100 ms; keeps the first value seen in each second. */
  public void record(long monoMillis, int handshaking) {
    long sec = Math.floorDiv(monoMillis, 1000);
    if (sec == lastSecond) {
      return;
    }
    lastSecond = sec;
    ring[next] = handshaking;
    next = (next + 1) % SAMPLES;
    count = Math.min(count + 1, SAMPLES);
  }

  public boolean rising() {
    if (count < SAMPLES) {
      return false;
    }
    for (int i = 1; i < SAMPLES; i++) {
      if (ring[(next + i) % SAMPLES] <= ring[(next + i - 1) % SAMPLES]) {
        return false;
      }
    }
    return true;
  }
}
