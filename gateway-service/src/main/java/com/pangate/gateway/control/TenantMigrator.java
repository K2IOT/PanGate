package com.pangate.gateway.control;

import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.LongSupplier;

/**
 * Moves a tenant's sessions off this cell after a CellMap change (spec §6.8): every 5 s each loop sends
 * {@code RECONNECT{re_bootstrap=true, delay ∈ [0, 5000] ms}} to 2% of the tenant's sessions it had when migration
 * started (at least one). Stops when the tenant maps back here or no ACTIVE session of it is left.
 */
public class TenantMigrator {

  private record Migration(int[] batch, AtomicIntegerArray left) {}

  private final LoopRegistry loops;
  private final CellMapView cells;
  private final LongSupplier randomDelayMs;
  private final ConcurrentHashMap<String, Migration> migrating = new ConcurrentHashMap<>();

  public TenantMigrator(LoopRegistry loops, CellMapView cells, LongSupplier randomDelayMs) {
    this.loops = loops;
    this.cells = cells;
    this.randomDelayMs = randomDelayMs;
  }

  public void start(String tenant) {
    migrating.computeIfAbsent(tenant, t -> {
      int[] batch = new int[loops.size()];
      java.util.Arrays.fill(batch, -1);
      AtomicIntegerArray left = new AtomicIntegerArray(loops.size());
      for (int i = 0; i < loops.size(); i++) {
        left.set(i, -1);
      }
      return new Migration(batch, left);
    });
  }

  public void stop(String tenant) {
    migrating.remove(tenant);
  }

  public Set<String> migrating() {
    return Set.copyOf(migrating.keySet());
  }

  /** gw-tick, every 5 s. */
  public void tick() {
    migrating.forEach((tenant, m) -> {
      if (cells.servesTenant(tenant) || done(m)) {
        migrating.remove(tenant, m);
        return;
      }
      for (LoopContext c : loops.all()) {
        c.eventLoop().execute(() -> step(c, tenant, m));
      }
    });
  }

  private static boolean done(Migration m) {
    for (int i = 0; i < m.left().length(); i++) {
      if (m.left().get(i) != 0) {
        return false;
      }
    }
    return true;
  }

  private void step(LoopContext c, String tenant, Migration m) {
    List<Session> active = new ArrayList<>();
    c.forEachSession(s -> {
      if (s.state() == SessionState.ACTIVE && s.identity() != null && tenant.equals(s.identity().tenantId())) {
        active.add(s);
      }
    });
    int idx = c.index();
    if (m.batch()[idx] < 0) {
      m.batch()[idx] = Math.max(1, (int) Math.ceil(active.size() * 0.02));
    }
    int n = Math.min(m.batch()[idx], active.size());
    for (int i = 0; i < n; i++) {
      active.get(i).drain(randomDelayMs.getAsLong(), true);
    }
    m.left().set(idx, active.size() - n);
  }
}
