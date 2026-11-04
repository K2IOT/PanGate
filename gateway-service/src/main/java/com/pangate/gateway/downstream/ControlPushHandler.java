package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.ConnRef;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.List;

/**
 * Control push (spec §6.5) for every session of one principal on one loop: SUBSCRIBE (tenant namespace only, no
 * pattern check, still capped), UNSUBSCRIBE, KICK (4012 for older sessions) and REFRESH_CONFIG. Owner loop only.
 */
public final class ControlPushHandler {

  private final PrincipalIndex principals;
  private final TenantLimitsStore tenants;
  private final Counter subscribe;
  private final Counter unsubscribe;
  private final Counter kick;
  private final Counter refresh;
  private final Counter rejected;

  public ControlPushHandler(PrincipalIndex principals, TenantLimitsStore tenants, GatewayMetrics m) {
    this.principals = principals;
    this.tenants = tenants;
    this.subscribe = m.counter("gw.control.push", "op", "subscribe");
    this.unsubscribe = m.counter("gw.control.push", "op", "unsubscribe");
    this.kick = m.counter("gw.control.push", "op", "kick");
    this.refresh = m.counter("gw.control.push", "op", "refresh_config");
    this.rejected = m.counter("gw.subscribe.rejected", "reason", "control");
  }

  public void apply(LoopContext loop, String principal, ControlOp op) {
    switch (op.getOpCase()) {
      case SUBSCRIBE -> {
        subscribe.increment();
        for (Session s : sessions(loop, principal)) {
          int cap = tenants.policy(s.identity().tenantId()).topicsPerConn();
          for (String t : op.getSubscribe().getTopicsList()) {
            if (t.length() > TopicPatterns.MAX_TOPIC_LENGTH || !TopicPatterns.inTenant(s.identity().tenantId(), t)) {
              rejected.increment();
            } else if (s.hasTopic(t) || (s.topics() == null ? 0 : s.topics().length) < cap) {
              loop.topics().add(s, t);
            } else {
              rejected.increment();
            }
          }
        }
      }
      case UNSUBSCRIBE -> {
        unsubscribe.increment();
        for (Session s : sessions(loop, principal)) {
          op.getUnsubscribe().getTopicsList().forEach(t -> loop.topics().remove(s, t));
        }
      }
      case KICK -> kick(loop, principal, op.getKick().getNewConnId(), op.getKick().getConnectedAtMs());
      case REFRESH_CONFIG -> refresh.increment();  // no per-principal limit cache exists to drop
      default -> { }
    }
  }

  /**
   * Single-session order (spec §6.6): a connection is older when it connected earlier; on the same millisecond the
   * smaller connId (ULID-like, time-sortable) is older. Total, so two sessions never spare each other.
   */
  public static boolean older(long atMs, String connId, long otherAtMs, String otherConnId) {
    return atMs < otherAtMs || (atMs == otherAtMs && connId.compareTo(otherConnId) < 0);
  }

  /** Closes 4012 every session of the principal that is {@link #older} than {@code newConnId}. */
  public void kick(LoopContext loop, String principal, String newConnId, long connectedAtMs) {
    kick.increment();
    for (Session s : sessions(loop, principal)) {
      if (!s.connId().equals(newConnId) && older(s.connectedAtMs(), s.connId(), connectedAtMs, newConnId)) {
        s.close(CloseCode.DUPLICATE_SESSION, "KICKED");
      }
    }
  }

  private List<Session> sessions(LoopContext loop, String principal) {
    List<Session> out = new ArrayList<>(2);
    for (ConnRef ref : principals.get(principal)) {
      if (ref.loopIdx() != loop.index()) {
        continue;
      }
      Session s = loop.session(ref.sessionKey());
      if (s != null && s.identity() != null && principal.equals(s.identity().principalId())
          && (s.state() == SessionState.ACTIVE || s.state() == SessionState.DRAINING)) {
        out.add(s);
      }
    }
    return out;
  }
}
