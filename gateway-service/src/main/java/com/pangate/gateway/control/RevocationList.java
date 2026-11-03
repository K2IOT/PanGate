package com.pangate.gateway.control;

import com.pangate.contracts.v1.Revoke;
import com.pangate.gateway.auth.ResumeTicketService;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Revoked principals, devices and token ids (spec §3.4, §6.8), fed by gw.control and read on HELLO and REAUTH.
 * Keys: {@code p:<principal>}, {@code d:<principal>/<device>}, {@code j:<jtiHash>}; value = expiry (epoch ms).
 * Not hard-capped: the {@code gw.revocations} gauge is alerted on instead (a dropped revocation is worse).
 */
public final class RevocationList {

  private final ConcurrentHashMap<String, Long> entries = new ConcurrentHashMap<>();

  public static String principalKey(String principal) {
    return "p:" + principal;
  }

  public static String deviceKey(String principal, String device) {
    return "d:" + principal + "/" + device;
  }

  public static String jtiKey(long jtiHash) {
    return "j:" + jtiHash;
  }

  public static Optional<String> keyOf(Revoke r) {
    return switch (r.getTargetCase()) {
      case PRINCIPAL_ID -> r.getPrincipalId().isEmpty() ? Optional.empty()
          : Optional.of(principalKey(r.getPrincipalId()));
      case DEVICE -> r.getDevice().getPrincipalId().isEmpty() || r.getDevice().getDeviceId().isEmpty()
          ? Optional.empty() : Optional.of(deviceKey(r.getDevice().getPrincipalId(), r.getDevice().getDeviceId()));
      case JTI -> r.getJti().isEmpty() ? Optional.empty() : Optional.of(jtiKey(ResumeTicketService.jtiHash(r.getJti())));
      case TARGET_NOT_SET -> Optional.empty();
    };
  }

  public void add(String key, long expiresAtMs) {
    entries.merge(key, expiresAtMs, Math::max);
  }

  public void remove(String key) {
    entries.remove(key);
  }

  public boolean isRevoked(String principal, String device, long jtiHash, long nowMs) {
    if (entries.isEmpty()) {
      return false;
    }
    return live(principalKey(principal), nowMs) || live(deviceKey(principal, device), nowMs)
        || (jtiHash != 0 && live(jtiKey(jtiHash), nowMs));
  }

  private boolean live(String key, long nowMs) {
    Long exp = entries.get(key);
    return exp != null && exp > nowMs;
  }

  public int sweep(long nowMs) {
    int[] removed = {0};
    entries.entrySet().removeIf(e -> {
      boolean expired = e.getValue() <= nowMs;
      if (expired) {
        removed[0]++;
      }
      return expired;
    });
    return removed[0];
  }

  public int size() {
    return entries.size();
  }
}
