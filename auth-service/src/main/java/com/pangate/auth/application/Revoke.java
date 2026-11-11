package com.pangate.auth.application;

import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.ControlEncoder;
import com.pangate.auth.application.port.Outbox;
import com.pangate.auth.application.port.RevocationStore;
import com.pangate.auth.application.port.Transactions;
import com.pangate.auth.domain.Caller;
import com.pangate.auth.domain.ClaimLimits;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Revocation;
import com.pangate.auth.domain.Revocation.Kind;
import com.pangate.auth.domain.Scope;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * {@code POST /v1/revocations} and client disabling (spec §4.4, §4.5): the revocation row and its outbox record are
 * written in one transaction, so a revocation accepted while Kafka is down is still published later.
 */
public final class Revoke implements ManageClients.ClientRevoker {

  static final int JTI_MAX = 256;

  private final RevocationStore store;
  private final Outbox outbox;
  private final ControlEncoder encoder;
  private final Transactions tx;
  private final Clock clock;
  private final Duration ttl;
  private final String topic;

  public Revoke(RevocationStore store, Outbox outbox, ControlEncoder encoder, Transactions tx, Clock clock,
      Duration ttl, String topic) {
    this.store = store;
    this.outbox = outbox;
    this.encoder = encoder;
    this.tx = tx;
    this.clock = clock;
    this.ttl = ttl;
    this.topic = topic;
  }

  public Revocation principal(Caller caller, String principal) {
    caller.require(Scope.SESSIONS_REVOKE);
    checkPrincipal(caller, principal);
    return write(caller.tenant(), Kind.PRINCIPAL, Revocation.controlKey(Kind.PRINCIPAL, principal, null, null, null),
        caller.clientId());
  }

  public Revocation device(Caller caller, String principal, String device) {
    caller.require(Scope.SESSIONS_REVOKE);
    checkPrincipal(caller, principal);
    if (!ClaimLimits.validDevice(device)) {
      throw DomainException.badRequest("device_invalid", "device must be 1.." + ClaimLimits.DEVICE_MAX
          + " bytes without '/'");
    }
    return write(caller.tenant(), Kind.DEVICE, Revocation.controlKey(Kind.DEVICE, principal, device, null, null),
        caller.clientId());
  }

  /** A jti carries no tenant, so only the scope guards it; created_by records who asked (spec §4.4). */
  public Revocation jti(Caller caller, String jti) {
    caller.require(Scope.SESSIONS_REVOKE);
    if (jti == null || jti.isBlank() || bytes(jti) > JTI_MAX) {
      throw DomainException.badRequest("jti_invalid", "jti must be 1.." + JTI_MAX + " bytes");
    }
    return write(caller.tenant(), Kind.JTI, Revocation.controlKey(Kind.JTI, null, null, jti, null), caller.clientId());
  }

  public Revocation client(String actor, String clientId, String tenantId) {
    return write(tenantId, Kind.CLIENT, Revocation.controlKey(Kind.CLIENT, null, null, null, clientId), actor);
  }

  @Override
  public void revokeClient(String actor, String clientId, String tenantId) {
    client(actor, clientId, tenantId);
  }

  private Revocation write(String tenant, Kind kind, String key, String createdBy) {
    Instant now = clock.now();
    Revocation[] out = new Revocation[1];
    tx.inTx(() -> {
      out[0] = store.upsert(new Revocation(0, tenant, kind, key, now.plus(ttl), createdBy));
      outbox.append(topic, key, encoder.revoke(out[0], now), tenant);
    });
    return out[0];
  }

  private static void checkPrincipal(Caller caller, String principal) {
    String prefix = caller.tenant() + ":";
    if (principal == null || !principal.startsWith(prefix) || principal.length() == prefix.length()) {
      throw new DomainException("principal_outside_tenant", 403, "principal must start with '" + prefix + "'");
    }
    if (bytes(principal) > ClaimLimits.PRINCIPAL_MAX) {
      throw DomainException.badRequest("principal_too_long", "principal exceeds " + ClaimLimits.PRINCIPAL_MAX
          + " bytes");
    }
  }

  private static int bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8).length;
  }
}
