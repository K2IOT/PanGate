package com.pangate.gateway.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/** IPv4 / IPv6 CIDR blocks ("10.0.0.0/8", "2001:db8::/32"; a bare address is /32 or /128). Immutable. */
public final class CidrSet {

  private record Block(byte[] network, int prefix) {}

  private final List<Block> blocks;

  private CidrSet(List<Block> blocks) {
    this.blocks = blocks;
  }

  /** @throws IllegalArgumentException naming the first malformed entry */
  public static CidrSet parse(List<String> cidrs) {
    List<Block> out = new ArrayList<>();
    for (String c : cidrs) {
      String s = c.trim();
      int slash = s.indexOf('/');
      String host = slash < 0 ? s : s.substring(0, slash);
      byte[] addr = literal(host, c);
      int max = addr.length * 8;
      int prefix;
      try {
        prefix = slash < 0 ? max : Integer.parseInt(s.substring(slash + 1));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("invalid CIDR '" + c + "'", e);
      }
      if (prefix < 0 || prefix > max) {
        throw new IllegalArgumentException("invalid CIDR '" + c + "': prefix must be 0.." + max);
      }
      out.add(new Block(addr, prefix));
    }
    return new CidrSet(List.copyOf(out));
  }

  /** Only IP literals: a hostname must never trigger a DNS lookup here. */
  private static byte[] literal(String host, String entry) {
    if (host.isEmpty() || !(host.contains(":") || host.chars().allMatch(ch -> ch == '.' || Character.isDigit(ch)))) {
      throw new IllegalArgumentException("invalid CIDR '" + entry + "': not an IP literal");
    }
    try {
      return InetAddress.getByName(host).getAddress();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("invalid CIDR '" + entry + "'", e);
    }
  }

  public boolean contains(InetAddress a) {
    byte[] x = a.getAddress();
    for (Block b : blocks) {
      if (b.network().length == x.length && matches(b.network(), x, b.prefix())) {
        return true;
      }
    }
    return false;
  }

  private static boolean matches(byte[] net, byte[] x, int prefix) {
    int full = prefix / 8;
    for (int i = 0; i < full; i++) {
      if (net[i] != x[i]) {
        return false;
      }
    }
    int rest = prefix % 8;
    if (rest == 0) {
      return true;
    }
    int mask = (0xFF << (8 - rest)) & 0xFF;
    return (net[full] & mask) == (x[full] & mask);
  }
}
