package com.pangate.gateway.trace;

import com.pangate.gateway.control.ControlEventHandler;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.session.ConnRef;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionListener;
import com.pangate.gateway.session.SessionState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Kill switch trace.principal (spec §7.8): which principals are traced and until when, and the {@code traced} flag of
 * their live sessions — set at activation, and on their owner loops when a control event or the expiry changes it.
 */
public final class TraceTargets implements ControlEventHandler, SessionListener {

  public static final Duration MAX_TTL = Duration.ofMinutes(15);

  private final Map<String, Long> until = new ConcurrentHashMap<>();
  private final PrincipalIndex principals;
  private final LoopRegistry loops;
  private final LongSupplier wallMillis;

  public TraceTargets(PrincipalIndex principals, LoopRegistry loops, PrincipalHasher hasher, LongSupplier wallMillis) {
    this.principals = principals;
    this.loops = loops;
    this.wallMillis = wallMillis;
    SessionTrace.install(hasher);
  }

  public void put(String principal, long issuedAtMs, long expiresAtMs) {
    long cap = issuedAtMs + MAX_TTL.toMillis();
    until.put(principal, expiresAtMs > 0 ? Math.min(expiresAtMs, cap) : cap);
  }

  public void remove(String principal) {
    until.remove(principal);
  }

  public boolean traced(String principal, long nowMs) {
    Long u = until.get(principal);
    return u != null && nowMs < u;
  }

  /** Removes and returns the principals whose trace expired. */
  public List<String> sweep(long nowMs) {
    List<String> expired = new ArrayList<>();
    until.forEach((p, u) -> {
      if (nowMs >= u && until.remove(p, u)) {
        expired.add(p);
      }
    });
    return expired;
  }

  /** gw-tick, every 10 s: expired traces stop logging. */
  public void sweepExpired() {
    for (String p : sweep(wallMillis.getAsLong())) {
      flag(p, false);
    }
  }

  @Override
  public void onTrace(String principal, long issuedAtMs, long expiresAtMs) {
    put(principal, issuedAtMs > 0 ? issuedAtMs : wallMillis.getAsLong(), expiresAtMs);
    flag(principal, traced(principal, wallMillis.getAsLong()));
  }

  @Override
  public void onTraceRemoved(String principal) {
    remove(principal);
    flag(principal, false);
  }

  private void flag(String principal, boolean on) {
    for (ConnRef ref : principals.get(principal)) {
      LoopContext c = loops.get(ref.loopIdx());
      c.eventLoop().execute(() -> {
        Session s = c.session(ref.sessionKey());
        if (s != null && s.identity() != null && principal.equals(s.identity().principalId())
            && s.state() != SessionState.CLOSED) {
          s.traced(on);
        }
      });
    }
  }

  @Override
  public void activated(Session s) {
    if (traced(s.identity().principalId(), wallMillis.getAsLong())) {
      s.traced(true);
      SessionTrace.activated(s);
    }
  }

  @Override
  public void closed(Session s) {
    if (s.traced()) {
      SessionTrace.closed(s, s.closeCode(), s.closeReason());
    }
  }
}
