package com.pangate.gateway.tenant;

import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionListener;
import io.micrometer.core.instrument.Counter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntSupplier;

/**
 * Tenant connection cap (spec §4.6 step i): a local quota {@code ceil(maxConns × 1.1 / nodesInCell)}, then slots
 * borrowed through Redis. Also the session listener that gives a reserved slot back when the session closes.
 */
public final class TenantCap implements SessionListener, com.pangate.gateway.control.ControlEventHandler {

  /** Borrows one slot beyond the local quota; completes false when the cell-wide total would exceed maxConns. */
  @FunctionalInterface
  public interface Borrower {
    CompletionStage<Boolean> borrow(String tenant, int maxConns, int ownTotalAfter);
  }

  private final TenantConnCounter counter;
  private final IntSupplier nodes;
  private final Borrower borrower;
  private final Counter borrowOk;
  private final Counter borrowDenied;
  private final Counter borrowError;

  public TenantCap(TenantConnCounter counter, IntSupplier nodes, Borrower borrower, GatewayMetrics m) {
    this.counter = counter;
    this.nodes = nodes;
    this.borrower = borrower;
    this.borrowOk = m.counter("gw.tenant.borrow", "result", "ok");
    this.borrowDenied = m.counter("gw.tenant.borrow", "result", "denied");
    this.borrowError = m.counter("gw.tenant.borrow", "result", "error");
  }

  private com.pangate.gateway.loop.LoopRegistry loops;
  private com.pangate.gateway.control.TenantLimitsStore tenants;

  /** With loops and tenants: a cap switched on later also counts the sessions that are already open. */
  public TenantCap(TenantConnCounter counter, IntSupplier nodes, Borrower borrower, GatewayMetrics m,
      com.pangate.gateway.loop.LoopRegistry loops, com.pangate.gateway.control.TenantLimitsStore tenants) {
    this(counter, nodes, borrower, m);
    this.loops = loops;
    this.tenants = tenants;
  }

  /**
   * A tenant config changed (any scope): sessions of tenants that now have a cap but were opened uncounted are
   * counted, on their owner loops. Counting can exceed the quota; new HELLOs then borrow or are refused.
   */
  @Override
  public void onTenantCfg(com.pangate.contracts.v1.TenantCfg cfg) {
    if (loops == null) {
      return;
    }
    for (com.pangate.gateway.loop.LoopContext c : loops.all()) {
      c.eventLoop().execute(() -> c.forEachSession(s -> {
        if (s.identity() != null && !s.tenantReserved() && !s.warmUp()
            && s.state() != com.pangate.gateway.session.SessionState.CLOSED
            && tenants.policy(s.identity().tenantId()).maxConns() > 0) {
          counter.reserveBorrowed(s.identity().tenantId());
          s.tenantReserved(true);
        }
      }));
    }
  }

  /** No Redis: only the local quota of a one-node cell (unit tests, pipelines built without the cap). */
  public static TenantCap localOnly(GatewayMetrics m) {
    return new TenantCap(new TenantConnCounter(), () -> 1,
        (t, max, own) -> CompletableFuture.completedFuture(false), m);
  }

  public TenantConnCounter counter() {
    return counter;
  }

  public boolean tryReserve(String tenant, int maxConns) {
    return counter.tryReserve(tenant, TenantCapBorrower.quota(maxConns, nodes.getAsInt()));
  }

  /**
   * Reserves the slot first, so concurrent borrows on this node each send a higher total, then asks Redis; on deny,
   * error or timeout the slot is given back. Never fails: a Redis error completes false (spec §10). A true result
   * leaves the slot reserved: the caller releases it if the session can no longer be activated.
   */
  public CompletionStage<Boolean> borrow(String tenant, int maxConns) {
    counter.reserveBorrowed(tenant);
    CompletionStage<Boolean> s;
    try {
      s = borrower.borrow(tenant, maxConns, counter.count(tenant));
    } catch (RuntimeException e) {
      s = CompletableFuture.failedFuture(e);
    }
    return s.handle((ok, e) -> {
      boolean granted = e == null && Boolean.TRUE.equals(ok);
      (e != null ? borrowError : granted ? borrowOk : borrowDenied).increment();
      if (!granted) {
        counter.release(tenant);
      }
      return granted;
    });
  }

  /** Gives back a slot taken by {@link #tryReserve} or a granted {@link #borrow} that was never activated. */
  public void release(String tenant) {
    counter.release(tenant);
  }

  @Override
  public void activated(Session s) {}

  @Override
  public void closed(Session s) {
    if (s.tenantReserved()) {
      s.tenantReserved(false);
      counter.release(s.identity().tenantId());
    }
  }
}
