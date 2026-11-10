package com.pangate.pushapi.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * What Push API keeps from {@code gw.control} (push-api spec §4.1): revoked jtis, blocked API clients, and tenant
 * policy merged defaults ← fleet ← cell (this cell only) ← tenant, where only fields that are present override.
 * Session revocations ({@code revoke:p}, {@code revoke:d}), {@code cellmap} and {@code trace} belong to the gateway.
 */
public final class ControlState {

  private record Revocation(long issuedAtMs, long expiresAtMs) {
    boolean live(long now) {
      return expiresAtMs == 0 || now < expiresAtMs;
    }
  }

  private final String cell;
  private final TenantPolicy defaults;
  private final LongSupplier wallMs;
  private final Map<String, Revocation> jtis = new ConcurrentHashMap<>();
  private final Map<String, Revocation> clients = new ConcurrentHashMap<>();
  private final Map<String, TenantCfg> tenantCfgs = new ConcurrentHashMap<>();
  private volatile TenantCfg fleet;
  private volatile TenantCfg cellCfg;
  private volatile Map<String, TenantPolicy> policies = Map.of();
  private volatile TenantPolicy base;

  public ControlState(String cell, TenantPolicy defaults, LongSupplier wallMs) {
    this.cell = cell;
    this.defaults = defaults;
    this.wallMs = wallMs;
    this.base = defaults;
  }

  /** {@code value} null or empty is a tombstone. @throws IllegalArgumentException for a malformed value */
  public void apply(String key, byte[] value) {
    if (key == null) {
      return;
    }
    boolean tombstone = value == null || value.length == 0;
    if (key.startsWith("revoke:j:") || key.startsWith("revoke:c:")) {
      Map<String, Revocation> target = key.startsWith("revoke:j:") ? jtis : clients;
      String id = key.substring(9);
      if (tombstone) {
        target.remove(id);
        return;
      }
      ControlEvent e = parse(value);
      target.put(id, new Revocation(e.getIssuedAtMs(), e.getRevoke().getExpiresAtMs()));
      return;
    }
    if (key.equals("tenantcfg:fleet")) {
      fleet = tombstone ? null : parse(value).getTenantCfg();
    } else if (key.startsWith("tenantcfg:cell:")) {
      if (!key.substring(15).equals(cell)) {
        return;
      }
      cellCfg = tombstone ? null : parse(value).getTenantCfg();
    } else if (key.startsWith("tenantcfg:tenant:")) {
      String t = key.substring(17);
      if (tombstone) {
        tenantCfgs.remove(t);
      } else {
        tenantCfgs.put(t, parse(value).getTenantCfg());
      }
    } else {
      return;   // revoke:p / revoke:d / cellmap / trace / unknown: not Push API's
    }
    rebuild();
  }

  private static ControlEvent parse(byte[] value) {
    try {
      return ControlEvent.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("malformed ControlEvent", e);
    }
  }

  private synchronized void rebuild() {
    TenantPolicy b = merge(merge(defaults, fleet), cellCfg);
    Map<String, TenantPolicy> next = new ConcurrentHashMap<>();
    tenantCfgs.forEach((t, c) -> next.put(t, merge(b, c)));
    base = b;
    policies = next;
  }

  static TenantPolicy merge(TenantPolicy p, TenantCfg cfg) {
    if (cfg == null) {
      return p;
    }
    TenantLimits l = cfg.getLimits();
    KillSwitches k = cfg.getSwitches();
    return new TenantPolicy(
        k.hasTenantBlocked() ? k.getTenantBlocked() : p.blocked(),
        k.hasBroadcastEnabled() ? k.getBroadcastEnabled() : p.broadcastEnabled(),
        l.hasPushRate() && l.getPushRate() > 0 ? l.getPushRate() : p.pushRate(),
        l.hasPushBurst() && l.getPushBurst() > 0 ? l.getPushBurst() : p.pushBurst(),
        l.hasBroadcastRate() && l.getBroadcastRate() > 0 ? l.getBroadcastRate() : p.broadcastRate());
  }

  public boolean jtiRevoked(String jti) {
    Revocation r = jtis.get(jti);
    return r != null && r.live(wallMs.getAsLong());
  }

  /** Tokens of {@code clientId} issued before the revocation event ({@code iatMs < issued_at_ms}) are blocked. */
  public boolean clientBlocked(String clientId, long iatMs) {
    Revocation r = clients.get(clientId);
    return r != null && r.live(wallMs.getAsLong()) && iatMs < r.issuedAtMs();
  }

  public TenantPolicy policy(String tenant) {
    TenantPolicy p = policies.get(tenant);
    return p != null ? p : base;
  }

  /** Drops expired revocations (every minute). */
  public void prune() {
    long now = wallMs.getAsLong();
    jtis.values().removeIf(r -> !r.live(now));
    clients.values().removeIf(r -> !r.live(now));
  }

  /** Revocations currently held (metric {@code pushapi.revocations.active}). */
  public int revocations() {
    return jtis.size() + clients.size();
  }
}
