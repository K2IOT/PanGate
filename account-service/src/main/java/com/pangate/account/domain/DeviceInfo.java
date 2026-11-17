package com.pangate.account.domain;

import java.util.Set;

/** What the client says about the device that logs in (account spec §3 sessions): name 1..64, a known platform. */
public record DeviceInfo(String deviceName, String platform) {

  public static final Set<String> PLATFORMS = Set.of("ios", "android", "web", "desktop", "other");

  public static DeviceInfo of(String name, String platform) {
    String n = name == null ? "" : name.trim();
    int len = n.codePointCount(0, n.length());
    if (len < 1 || len > 64 || n.codePoints().anyMatch(Character::isISOControl)) {
      throw new DomainException("invalid_request", "deviceName is 1..64 characters");
    }
    if (platform == null || !PLATFORMS.contains(platform)) {
      throw new DomainException("invalid_request", "platform is one of " + PLATFORMS);
    }
    return new DeviceInfo(n, platform);
  }
}
