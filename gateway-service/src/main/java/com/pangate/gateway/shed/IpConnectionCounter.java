package com.pangate.gateway.shed;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.HexFormat;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concurrent connections per client address (spec §7.1). IPv6 is keyed by its /64, IPv4-mapped IPv6 by the IPv4
 * address. Updated only on connect and close.
 */
public final class IpConnectionCounter {

  private final ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();

  public boolean tryIncrement(InetAddress addr, int limit) {
    boolean[] ok = {false};
    counts.compute(key(addr), (k, v) -> {
      int current = v == null ? 0 : v;
      if (current >= limit) {
        return v;
      }
      ok[0] = true;
      return current + 1;
    });
    return ok[0];
  }

  public void decrement(InetAddress addr) {
    counts.computeIfPresent(key(addr), (k, v) -> v <= 1 ? null : v - 1);
  }

  public int count(InetAddress addr) {
    return counts.getOrDefault(key(addr), 0);
  }

  public int trackedKeys() {
    return counts.size();
  }

  public static String key(InetAddress addr) {
    byte[] b = addr.getAddress();
    if (addr instanceof Inet4Address) {
      return "4:" + addr.getHostAddress();
    }
    if (addr instanceof Inet6Address && isV4Mapped(b)) {
      return "4:" + (b[12] & 0xFF) + "." + (b[13] & 0xFF) + "." + (b[14] & 0xFF) + "." + (b[15] & 0xFF);
    }
    return "6:" + HexFormat.of().formatHex(b, 0, 8);
  }

  private static boolean isV4Mapped(byte[] b) {
    for (int i = 0; i < 10; i++) {
      if (b[i] != 0) {
        return false;
      }
    }
    return b[10] == (byte) 0xFF && b[11] == (byte) 0xFF;
  }
}
