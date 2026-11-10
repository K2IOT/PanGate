package com.pangate.auth.domain;

import java.time.Instant;
import java.util.Locale;

/** A session or client revocation published on gw.control under {@code controlKey} until {@code expiresAt}. */
public record Revocation(long id, String tenantId, Kind kind, String controlKey, Instant expiresAt, String createdBy) {

  public enum Kind {
    PRINCIPAL, DEVICE, JTI, CLIENT;

    public String db() {
      return name().toLowerCase(Locale.ROOT);
    }

    public static Kind fromDb(String s) {
      return valueOf(s.toUpperCase(Locale.ROOT));
    }
  }

  /** gw.control record keys (control.proto): revoke:p:, revoke:d:&lt;principal&gt;/&lt;device&gt;, revoke:j:, revoke:c:. */
  public static String controlKey(Kind kind, String principal, String device, String jti, String clientId) {
    return switch (kind) {
      case PRINCIPAL -> "revoke:p:" + principal;
      case DEVICE -> "revoke:d:" + principal + "/" + device;
      case JTI -> "revoke:j:" + jti;
      case CLIENT -> "revoke:c:" + clientId;
    };
  }
}
