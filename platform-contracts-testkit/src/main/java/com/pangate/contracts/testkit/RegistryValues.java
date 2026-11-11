package com.pangate.contracts.testkit;

import java.util.List;
import java.util.Optional;

/**
 * Registry value {@code reg:{principal}} → connId → value (push contracts spec §4). v2 is
 * {@code nodeId|epoch|connectedAtMs|platform|device}; device is last and may contain '|'. v1 (gateway 0.1.0) has four
 * parts and never matches a device target. {@link #parse} is the reference parser every reader must agree with.
 */
public final class RegistryValues {

  /** A parsed entry; {@code device} is empty for v1. */
  public record Parsed(String nodeId, long epoch, long connectedAtMs, String platform, Optional<String> device) {}

  /** A reference case: {@code expected} empty means malformed. */
  public record Case(String name, String value, Optional<Parsed> expected) {}

  private RegistryValues() {}

  public static String v2(String nodeId, long epoch, long connectedAtMs, String platform, String device) {
    return nodeId + "|" + epoch + "|" + connectedAtMs + "|" + (platform == null ? "" : platform) + "|"
        + (device == null ? "" : device);
  }

  public static Optional<Parsed> parse(String value) {
    if (value == null) {
      return Optional.empty();
    }
    String[] p = value.split("\\|", 5);
    if (p.length < 4 || p[0].isEmpty()) {
      return Optional.empty();
    }
    if (!digits(p[1]) || !digits(p[2])) {
      return Optional.empty();
    }
    long epoch;
    long at;
    try {
      epoch = Long.parseLong(p[1]);
      at = Long.parseLong(p[2]);
    } catch (NumberFormatException e) {  // overflow
      return Optional.empty();
    }
    return Optional.of(new Parsed(p[0], epoch, at, p[3], p.length == 5 ? Optional.of(p[4]) : Optional.empty()));
  }

  /** Decimal, non-negative, ASCII digits only (no sign, no other Unicode digits). */
  static boolean digits(String s) {
    if (s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  /** True only for a v2 entry whose non-empty device equals {@code device}. */
  public static boolean matchesDevice(Parsed p, String device) {
    return device != null && !device.isEmpty() && p.device().filter(d -> !d.isEmpty()).map(device::equals)
        .orElse(false);
  }

  public static List<Case> cases() {
    return List.of(
        new Case("v1", "gw-1|7|1000|ios", Optional.of(new Parsed("gw-1", 7, 1000, "ios", Optional.empty()))),
        new Case("v1 empty platform", "gw-1|7|1000|", Optional.of(new Parsed("gw-1", 7, 1000, "", Optional.empty()))),
        new Case("v2", "gw-1|7|1000|ios|d1", Optional.of(new Parsed("gw-1", 7, 1000, "ios", Optional.of("d1")))),
        new Case("v2 device with pipes", "gw-1|7|1000|ios|dev|with|pipes",
            Optional.of(new Parsed("gw-1", 7, 1000, "ios", Optional.of("dev|with|pipes")))),
        new Case("v2 empty device", "gw-1|7|1000||", Optional.of(new Parsed("gw-1", 7, 1000, "", Optional.of("")))),
        new Case("too few parts", "gw-1|7", Optional.empty()),
        new Case("epoch not a number", "gw-1|x|1000|ios", Optional.empty()),
        new Case("negative time", "gw-1|7|-1|ios", Optional.empty()),
        new Case("empty node", "|7|1000|ios", Optional.empty()),
        new Case("empty value", "", Optional.empty()),
        new Case("signed number", "gw-1|+7|1000|ios", Optional.empty()),
        new Case("non-ASCII digit", "gw-1|\u0667|1000|ios", Optional.empty()),
        new Case("three parts", "gw-1|7|1000", Optional.empty()),
        new Case("v2 null platform as the gateway writes it", "gw|7|1000||d1",
            Optional.of(new Parsed("gw", 7, 1000, "", Optional.of("d1")))));
  }
}
