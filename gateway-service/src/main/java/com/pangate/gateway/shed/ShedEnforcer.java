package com.pangate.gateway.shed;

import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Shed level 5 (spec §7.3): on each loop, close 4008 the sessions holding the most outbound bytes until the loop is
 * below 90% of its budget share.
 */
public final class ShedEnforcer implements Runnable {

  private final LoopRegistry loops;
  private final OutboundBudget budget;

  public ShedEnforcer(LoopRegistry loops, OutboundBudget budget) {
    this.loops = loops;
    this.budget = budget;
  }

  @Override
  public void run() {
    relieve();
  }

  public void relieve() {
    long limit = (long) (0.9 * budget.shareBytes());
    for (LoopContext c : loops.all()) {
      c.eventLoop().execute(() -> relieve(c, limit));
    }
  }

  private static void relieve(LoopContext c, long limit) {
    List<Session> byPending = new ArrayList<>();
    long total = 0;
    for (Session s : c.dirtySessions()) {
      if (s.state() == SessionState.ACTIVE || s.state() == SessionState.DRAINING) {
        byPending.add(s);
        total += s.pendingOutboundBytes();
      }
    }
    byPending.sort(Comparator.comparingLong(Session::pendingOutboundBytes).reversed());
    for (Session s : byPending) {
      if (total < limit) {
        break;
      }
      total -= s.pendingOutboundBytes();
      s.close(CloseCode.SLOW_CONSUMER, "SHED");
    }
  }
}
