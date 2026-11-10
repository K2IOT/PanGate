package com.pangate.auth.domain;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Connect-token claim rules (spec §4.3). They mirror the gateway's JwtVerifier and ResumeTicket limits, so a token
 * Auth mints is never rejected by the gateway for its shape. Lengths are UTF-8 bytes.
 */
public final class ClaimLimits {

  public static final int TENANT_MAX = 64;
  public static final int PRINCIPAL_MAX = 256;
  public static final int DEVICE_MAX = 128;
  public static final int TOPICS_MAX = 8;
  public static final int TOPIC_MAX = 64;
  public static final int TTL_MIN = 60;
  public static final int TTL_MAX = 600;

  private ClaimLimits() {}

  public static ConnectTokenRequest validate(String tenant, String principal, String device, List<String> topics,
      Integer ttlSec) {
    if (tenant == null || tenant.isEmpty() || tenant.indexOf(':') >= 0 || bytes(tenant) > TENANT_MAX) {
      throw DomainException.badRequest("tenant_invalid", "tenant must be 1.." + TENANT_MAX + " bytes without ':'");
    }
    if (Tenant.isOperator(tenant) || Tenant.WARMUP.equals(tenant)) {
      throw new DomainException("tenant_not_allowed", 403, "tenant " + tenant + " cannot mint connect tokens");
    }
    String prefix = tenant + ":";
    if (principal == null || !principal.startsWith(prefix) || principal.length() == prefix.length()) {
      throw DomainException.badRequest("principal_outside_tenant", "principal must start with '" + prefix + "'");
    }
    if (bytes(principal) > PRINCIPAL_MAX) {
      throw DomainException.badRequest("principal_too_long", "principal exceeds " + PRINCIPAL_MAX + " bytes");
    }
    if (device == null || device.isEmpty() || bytes(device) > DEVICE_MAX) {
      throw DomainException.badRequest("device_invalid", "device must be 1.." + DEVICE_MAX + " bytes");
    }
    List<String> t = topics == null ? List.of() : topics;
    if (t.size() > TOPICS_MAX) {
      throw DomainException.badRequest("too_many_topics", "at most " + TOPICS_MAX + " topic patterns");
    }
    for (String p : t) {
      if (p == null || !p.startsWith(prefix)) {
        throw DomainException.badRequest("topic_outside_tenant", "topic patterns must start with '" + prefix + "'");
      }
      if (bytes(p) > TOPIC_MAX) {
        throw DomainException.badRequest("topic_pattern_too_long", "topic pattern exceeds " + TOPIC_MAX + " bytes");
      }
    }
    int ttl = ttlSec == null ? TTL_MAX : ttlSec;
    if (ttl < TTL_MIN || ttl > TTL_MAX) {
      throw DomainException.badRequest("ttl_out_of_range", "ttlSec must be in [" + TTL_MIN + ", " + TTL_MAX + "]");
    }
    return new ConnectTokenRequest(tenant, principal, device, List.copyOf(t), ttl);
  }

  static int bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8).length;
  }
}
